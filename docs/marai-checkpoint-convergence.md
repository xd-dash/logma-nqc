# Marai checkpoint convergence with NQC

Status: design only; gated on successful Huram exact-SHA Marai + Atman lifecycle qualification and disposable Agni/CoreOS qualification.

This document applies the existing NQC rule:

> Missing a hint is acceptable. Being unable to discover and repair the miss is not.

Marai does **not** use NQC as Redis replication, consensus, or live key synchronization. The first supported model has one authoritative mutation lineage and distributes immutable recovery checkpoints around it.

## Authority model

```text
one ACTIVE Marai authority
        |
        | create / rotate
        v
(authority era, sequence)
        |
        | explicit checkpoint lifecycle
        v
immutable MRS1 object
        +
authoritative checkpoint metadata
        |
        +---- best-effort hint ----> regional standby metadata/cache
        |
        `---- anti-entropy manifest/reconciliation
```

The live Marai process remains the only active cryptographic authority for its mutation lineage. An MRS1 object is inert recovery state until an explicit recovery operation creates a new Marai process/era.

## NQC state mapping

The CDN design separates `hint`, `confirmed`, and `head`. The Marai checkpoint adaptation uses the same epistemic split:

```text
hinted_checkpoint
    newest checkpoint identity learned from transient signaling

confirmed_checkpoint
    newest checkpoint identity verified against authoritative metadata

materialized_checkpoint
    newest confirmed checkpoint whose immutable MRS1 bytes were fetched and digest-verified locally
```

A hint never becomes confirmed merely because it carries a plausible era/sequence or digest.

## Checkpoint identity

A checkpoint identity must include enough information to reject ambiguity and rollback, conceptually:

```text
cell_id
origin_instance_id
origin_era
origin_seq
checkpoint_digest
object_generation_or_immutable_locator
format = MRS1
```

Ordering uses Marai authority lineage, not wall-clock timestamps.

Within one active era:

```text
(era, seq=20) < (era, seq=21)
```

Recovery creates a new era. A recovered process therefore does not reuse the predecessor revision domain:

```text
old authority: era 7 ... seq 23
checkpoint:    origin 7:23
new authority: era 8, seq 0
```

A deployment/control-plane identity must determine which new era is the legitimate successor. NQC does not decide competing writers.

## Authoritative commit point

MRS1 bytes and checkpoint metadata require a clear commit sequence:

```text
1. Marai reaches QUIESCED
2. terminal/explicit MRS1 bytes are produced
3. bytes are written to immutable storage
4. digest and immutable locator/generation are verified
5. authoritative checkpoint metadata is committed
6. only then publish best-effort checkpoint hints
```

A successful Pub/Sub publish is never the checkpoint commit point. A failed publish never rolls back a committed checkpoint.

## Hint protocol

A future versioned hint may contain only non-secret identity/evidence fields:

```json
{
  "v": 1,
  "cell_id": "...",
  "origin_era": "0000000000000007",
  "origin_seq": "0000000000000017",
  "digest": "sha256:...",
  "checkpoint_id": "..."
}
```

It must not contain:

- MRS1 bytes;
- recovery private keys;
- Marai ACL passwords;
- plaintext master-key material;
- arbitrary storage URLs supplied by an untrusted publisher.

The receiver resolves `checkpoint_id` through its configured authoritative checkpoint origin, as NQC already avoids trusting arbitrary origin URLs in hints.

## Reconciliation

On startup, Pub/Sub reconnect, local checkpoint loss, or periodically with jitter, a standby reconciles against an authoritative manifest.

Conceptually:

```text
manifest for cell
  current checkpoint identity
  immutable locator/generation
  digest
  lifecycle lineage
```

The standby:

1. authenticates/validates authoritative metadata;
2. monotonically advances `confirmed_checkpoint`;
3. fetches the immutable MRS1 object if local materialization is behind;
4. verifies exact digest/generation;
5. advances `materialized_checkpoint` only after verification.

Redis persistence for this NQC metadata remains optional because it is derived from the checkpoint authority. Redis restart means the standby has forgotten its cache and must reconcile before claiming readiness.

## Operation-specific consistency

NQC's stale-serving cache policy does not transfer wholesale to cryptographic authority.

A standby possessing immutable historical key versions may eventually support narrowly defined read-like operations, but the first model should keep standbys inert.

If replica execution is introduced later, minimum rules are:

```text
DECRYPT explicit old MRA1 version
    potentially safe on a replica that has the authenticated immutable key version

ENCRYPT / GENERATE_DATA_KEY
    require authority-confirmed current primary lineage

CREATE / ROTATE
    active writer only
```

Do not let a merely hinted or stale replica create new ciphertext under an old primary version.

## Failover versus active-active

NQC can make cross-region recovery/failover cheap:

```text
region A
  active Marai
  confirmed checkpoint 7:23

region B
  no active authority
  materialized checkpoint 7:23

A fails
  -> Huram selects exact recovery checkpoint
  -> separately authorizes recovery
  -> B creates NEW Marai era 8
```

It cannot safely provide:

```text
region A rotates independently
region B rotates independently
then eventual merge
```

That requires a separate writer-authority/consensus design. No NQC implementation should smuggle that requirement into Redis Pub/Sub or last-writer-wins metadata.

## Relationship to Logma

Logma is a suitable low-latency hint/lifecycle transport because its semantics already distinguish signal from destructive authority.

```text
checkpoint committed
    -> Logma/NQC hint

cell lifecycle deadline
    -> lifecycle shutdown signal
    -> Huram resolves exact cell/state
    -> quiesce/export/verify/zeroize/destroy
```

Neither signal is proof that the terminal artifact exists or that the old authority is gone.

## Qualification gate

Do not implement regional propagation until all of these are green:

```text
1. Marai production-image lifecycle suite
2. Marai real two-process MRS1 recovery suite
3. Atman readiness follows Marai ACTIVE state
4. Huram exact-SHA cross-repo local smoke
5. disposable Agni/CoreOS cell lifecycle
6. immutable checkpoint persist + independent verification
7. exact old-cell destruction + absence verification
8. explicit recovery into a new authority era
```

Only after that should the first NQC slice add:

```text
checkpoint metadata schema
best-effort Logma hint
standby hinted/confirmed/materialized state
startup/reconnect/periodic anti-entropy
negative tests for stale/reordered/forged hints
```

The NQC implementation remains optional acceleration and repair around authoritative immutable checkpoint state, never the cryptographic source of truth.

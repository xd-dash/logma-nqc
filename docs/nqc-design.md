# Not Quite Consistency (NQC): a Redis-Pub/Sub CDN protocol

Status: design only, not implemented. This document is the actionable follow-up to an
informal writeup reviewed against two sibling services, `marai` and `logma`, to ground it
in patterns already in production use.

## 1. Premise

Redis Pub/Sub is the fast path, not the truth. Missing a hint is acceptable; being unable
to *discover* that you missed one is not.

Each standalone Redis instance is an independent cache island. There is no Redis Cluster,
no replication relationship, and no requirement that islands agree at every moment. This
is not a limitation to work around — it's the deployment shape that keeps the model
operationally simple, and it has real precedent in this org: `marai` (an in-process Redis
module KMS) is explicitly designed to run only on a single standalone instance, because
module-owned state isn't replicated and doesn't survive a restart. NQC leans into the same
constraint for cached CDN bytes instead of key material.

```
                     authoritative writer
                            |
                     PUT object v=42
                            |
                  +---------+---------+
                  |                   |
             durable origin       Pub/Sub hint
                  |                   |
                  |        +----------+----------+
                  |        v          v           v
                  |     Redis A    Redis B     Redis C
                  |      v42        v41         v42
                  |
                  +------------ pull/reconcile ------------->
```

## 2. Relationship to `logma`

`logma` already relays Redis Pub/Sub messages to HTTP callbacks, and its own docs
(`docs/security-events.md`) state the exact premise above: Pub/Sub is "a low-latency
notification transport, not a durable replication log," and call for pairing it with a
durable catch-up mechanism. `logma`'s `internal/events` package independently arrived at
the same core primitive NQC needs — a `VersionStore` with `ApplyIfNewer`, i.e. a
monotonic `max(current, incoming)` merge over a versioned `Event`.

NQC is specified here as an **independent** design — `logma-nqc` does not import
`logma` (an unexported `internal/` package can't cross a module boundary anyway, and a
CDN cache and a security-event relay are different enough services that coupling them
would be a mistake). But the convergence is worth naming explicitly: it's evidence the
approach is sound, and a future implementation should mirror `logma`'s shape
(`Bus`-style interface, small typed `Event`/`Hint` struct, `internal/` package layout,
`fmt.Errorf("...: %w", err)` error wrapping, env-var config, no config framework) rather
than inventing new conventions.

## 3. Freshness states

```
FRESH            local object == latest known version
PROBABLY_FRESH   no evidence that local object is stale
KNOWN_STALE      observed a newer version but haven't fetched it yet
SHRUG            freshness cannot presently be established
```

`SHRUG` is part of the specification, not an error case. A node that can state "I don't
know if I'm fresh" is more correct than one that silently assumes it is.

## 4. Object model

Every object version is immutable once published:

```
nqc:data:<key>:<revision>          bytes + metadata
nqc:head:<key>          -> revision   (mutable pointer, per edge)
nqc:known:<key>         -> revision   (newest revision this edge has heard about)
nqc:tomb:<key>          -> revision   (deletion tombstone, see §7)
```

Metadata stored alongside `nqc:data:<key>:<revision>`:

```
revision
content_hash   (sha256, mandatory - see §8)
created_at
expires_at
content_type
etag
```

Rule: **never modify a published revision.** Create `N+1` and move the head. This alone
eliminates most of the update-race surface.

### Revision issuance must be durable

A single logical writer issues revisions, but that writer is a process that can crash and
restart. If revisions are an in-memory counter, a restart can reissue revision `107` with
different content than the `107` some edge already cached — a silent immutability
violation that no amount of downstream protocol correctness can repair.

Back revisions with something durable across restarts:

```
revision = <origin_epoch>:<seq>
```

where `seq` comes from the origin's own datastore (an autoincrement column, a durable
counter) and `origin_epoch` bumps on every process restart that can't prove it resumed
from the last durable `seq`. Edges compare revisions as `(epoch, seq)` tuples,
lexicographically. This turns "writer crashed" into "epoch bumped, ordering preserved,"
instead of "revision collision, correctness undefined."

## 5. Update protocol

Origin writes durably first, publishes second:

```
1. write object to authoritative storage: /foo.json rev=107 hash=sha256:abc...
2. only after (1) succeeds, publish on nqc:update:
   {"key": "/foo.json", "revision": 107, "hash": "abc...", "action": "put"}
```

Every Redis island subscribed to `nqc:update` treats the message as a hint, not an
obligation. Three subscriber strategies:

```
push-through   fetch the new revision immediately
invalidate     drop/mark the old local revision stale
lazy           record "107 exists", fetch on next request  <- default for a CDN
```

Lazy invalidation spreads origin load across real request traffic instead of creating a
synchronized stampede the moment a hint lands. The edge simply records:

```
nqc:known:/foo.json = 107      (from the hint)
nqc:head:/foo.json  = 106      (still serving this until asked for the new one)
```

### Hints are unordered, duplicated, and occasionally missing — by design

Never trust Pub/Sub message order. On receipt, an edge does:

```
nqc:known(key) = max(nqc:known(key), incoming.revision)
```

implemented atomically so concurrent hints can't race each other:

```lua
-- KEYS[1] = nqc:known:<key>, ARGV[1] = incoming revision (as a sortable string)
local cur = redis.call('GET', KEYS[1])
if (not cur) or (ARGV[1] > cur) then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return 0
```

Out-of-order delivery (`102, 104, 103`) and duplicate delivery both become harmless: the
max-merge is idempotent and commutative. This is what lets NQC treat Pub/Sub as exactly
as unreliable as it actually is, instead of pretending otherwise.

## 6. Read path

```
local  = GET nqc:head:<key>
known  = GET nqc:known:<key>
tomb   = GET nqc:tomb:<key>

local == known                 -> FRESH, return local
local <  known                 -> KNOWN_STALE, refresh (respecting §9), then return
local exists, known missing    -> PROBABLY_FRESH, return local
local missing, tomb >= wanted  -> object deleted, return 404 (never resurrect - §7)
local missing                  -> SHRUG, fetch from origin
origin unreachable             -> serve stale if policy allows, else fail
```

Availability beats freshness unless the caller opts out. Callers select behavior with an
explicit header-like knob:

```
Consistency: shrug          give me something reasonably recent, don't ask anyone
Consistency: probably       serve local unless locally known stale (default)
Consistency: latest-known   validate against nqc:known before serving
Consistency: origin         bypass NQC, hit the authority directly
```

## 7. Deletes need tombstones

A bare `DELETE key` with no version is dangerous: an edge holding an older cached copy
can resurrect it during reconciliation, because it has no way to know the delete happened
after its copy. Deletes are versioned events like any other write:

```
{"key": "/foo", "revision": 110, "action": "delete"}
```

stored as `nqc:tomb:/foo = 110`. An edge holding revision `108` can now see `108 < 110`
and knows not to treat its copy as current, and reconciliation can't resurrect it either
(the manifest carries the tombstone revision, not just live keys).

Tombstone retention must exceed the maximum reconciliation interval, and needs its own
bounded background sweep independent of the hot-object working set (e.g. retain 7 days if
reconciliation is guaranteed within 6 hours) — otherwise the tombstone set grows without
bound on a namespace with heavy churn.

## 8. Content integrity is mandatory, not advisory

`content_hash` must be verified on every fetch — origin-to-edge and, if ever added,
edge-to-edge — not merely recorded as metadata. On mismatch: quarantine the fetched bytes
under their claimed revision (never serve them, never let them silently overwrite a good
cached copy), emit a corruption metric, and treat the read as `SHRUG`.

## 9. Stampede control

When a hint or a request reveals `106 -> 107`, don't let every in-flight request trigger
an origin fetch:

```
SET nqc:fetch:<key> <request-id> NX PX 5000
```

Winner fetches `107`. Everyone else either serves `106` (stale-while-revalidate) or waits
a short bounded period for the winner to finish. "Someone else is probably fixing it."

The same lease pattern must guard **reconnect-triggered reconciliation**: an edge that
just recovered from a partition should re-check its uncertain shards immediately rather
than wait for the next periodic tick (to bound worst-case staleness), but that immediate
check needs to be jittered/leased too — a network blip that drops many edges at once must
not turn their simultaneous reconnect into a manifest-endpoint stampede.

## 10. Anti-entropy: the correctness backstop

Pub/Sub alone cannot provide eventual consistency — a disconnected subscriber simply
never sees the messages it missed, and Redis Pub/Sub keeps no replay log. Without a
second mechanism, NQC would just be "Redis Pub/Sub with extra steps." The anti-entropy
layer is what makes the eventual-repair claim true instead of aspirational.

Maintain a sharded manifest:

```
nqc:manifest:<shard>   (shard = hash(key) mod N)
```

A single monotonic "generation" counter per shard (as in the original sketch) tells an
edge *that* a shard changed but not *which* keys inside it moved, forcing a full-shard
refetch on any change. Prefer a **content digest per shard** instead — a hash over the
sorted `{key: revision}` pairs in that shard:

```
manifest:a7  digest = sha256({ "/foo.json": 109, "/foo/bar": 81, ... })
```

An edge compares its cached shard digest to the origin's. Mismatch → fetch just that
shard's `{key: revision}` table, diff key-by-key, refresh only what actually moved. This
scales far better than a monotonic counter once shards hold more than a handful of keys,
and it degrades gracefully under key skew (a hot shard's digest just changes often; it
never produces false negatives the way an aggregate counter can if updates coincide).

Combined:

```
Pub/Sub          milliseconds-to-seconds propagation
reconciliation   eventual, bounded-interval repair
```

No event replay log is required — an edge that's 20 revisions behind on a key jumps
directly from its last known revision to the manifest's current one. This is the
simplification that keeps NQC out of Kafka/Streams territory.

## 11. Redis operational hazards

Control-plane keys (`nqc:head`, `nqc:known`, `nqc:tomb`, `nqc:manifest`) are small and
must stay resident; cached object bytes (`nqc:data:*`) are large and disposable. Running
both under a single `allkeys-lru` maxmemory policy risks the control-plane keys getting
evicted under memory pressure from a large object working set. The failure mode is
safe-but-costly (an evicted `nqc:head` just looks like `local missing` → `SHRUG` →
fetch-from-origin) but is an availability/latency cliff that should be a deliberate
choice, not an accident. Prefer isolating control-plane keys in a separate logical DB (or
instance) from object bytes, or use `volatile-lru` with control keys left unexpired.

## 12. Pub/Sub authenticity

Nothing in Redis Pub/Sub itself stops a compromised or misconfigured client from
publishing a bogus high revision on `nqc:update` — which, given lazy invalidation, could
be used to make edges believe a nonexistent revision exists (denial of freshness) or to
trigger a stampede against a URL that then 404s. Restrict `PUBLISH` on `nqc:*` channels to
the origin's Redis ACL identity (the same fail-closed posture `marai` takes toward its own
ACL configuration), and treat a hint whose claimed revision can't subsequently be fetched
as a signal to fall back to `SHRUG` rather than wedge on `KNOWN_STALE` forever.

## 13. Optional multi-writer mode

For a CDN, one logical writer and many independent caches is the easy-to-reason-about
default and should stay the default. If multiple writers are genuinely required, use a
Hybrid-Logical-Clock-style tuple:

```
revision = <logical_clock>:<writer_id>
```

ordered lexicographically, with two rules stated explicitly (the original sketch left
both implicit, which is enough to make two edges disagree on which of two colliding
updates "won" if they received them in different orders):

- a stated maximum clock-skew bound between writers, and
- a deterministic tie-break — lexical `writer_id` compare when logical clocks are equal.

## 14. Consistency contract

```
NQC-0  "¯\_(ツ)_/¯"     serve local copy if present, no freshness check.
                    Fast, maximally available. Good fit: hashed-filename
                    static assets, images, fonts - anything effectively immutable.

NQC-1  "Probably"    serve local unless locally known stale (nqc:known says newer
                    exists). Default. Pub/Sub usually catches updates promptly;
                    anti-entropy repairs what it misses.

NQC-2  "Ask Mom"      validate against the manifest/head before serving bytes from
                    Redis. Good fit: config, feature flags, moderately sensitive
                    mutable metadata.

Actual consistency    bypass NQC, read the origin directly. NQC never pretends to
                    replace this option.
```

Formal-ish statement: *NQC guarantees monotonic convergence toward the authoritative
state whenever communication and reconciliation eventually resume. During a partition, a
node may serve arbitrarily stale but locally-valid data per its configured freshness
policy. Pub/Sub accelerates convergence but is never required for correctness —
reconciliation is.*

Operationally: **everyone eventually agrees, unless they don't, but if they don't, they
eventually notice.** The "eventually notice" half is a testable claim (§15), which is
what separates this from just throwing Pub/Sub at the problem and hoping.

## 15. Observability as a spec requirement

Because NQC's actual guarantee is about *eventually noticing* staleness rather than
preventing it, that guarantee is unfalsifiable without metrics. Treat these as part of
the protocol, not optional ops polish:

- staleness age (`now - object.created_at` at serve time), histogrammed per consistency
  tier (§14) and per key class
- `SHRUG` rate — a sustained rise means Pub/Sub or reconciliation is degraded
- reconciliation lag — time since each shard's digest last matched the origin
- tombstone-resurrection-prevented counter (§7) — near zero is healthy; nonzero and
  climbing means an edge's reconciliation interval is too loose relative to churn
- fetch-lease contention (§9) — confirms the stampede guard is actually engaging

## 16. Redis namespace summary

```
nqc:head:<key>          -> revision
nqc:known:<key>         -> newest observed revision
nqc:data:<key>:<rev>    -> bytes + metadata (immutable)
nqc:tomb:<key>          -> deletion revision
nqc:manifest:<shard>    -> {generation, digest}
nqc:fetch:<key>         -> refresh lease (NX PX)

channel: nqc:update      (put/delete hints; ACL-restricted to origin - §12)
```

## 17. Illustrative Go shapes

These are design-level sketches to guide a future implementation — not a package meant to
compile as-is, and deliberately not built on `logma/internal/events` (§2). Naming mirrors
`logma`'s conventions (small typed structs, `%w`-wrapped errors) so a future port is
mechanical rather than a rewrite.

```go
package nqc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Revision is (origin_epoch, seq), compared lexicographically — see §4.
type Revision struct {
	Epoch uint64
	Seq   uint64
}

func (r Revision) String() string { return fmt.Sprintf("%020d:%020d", r.Epoch, r.Seq) }

func (r Revision) Less(o Revision) bool {
	if r.Epoch != o.Epoch {
		return r.Epoch < o.Epoch
	}
	return r.Seq < o.Seq
}

type ObjectMeta struct {
	Key         string
	Revision    Revision
	ContentHash string // sha256, mandatory - §8
	ContentType string
	ETag        string
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

// Hint is an nqc:update / delete pub/sub payload. Never trust ordering - §5.
type Hint struct {
	Key      string   `json:"key"`
	Revision Revision `json:"revision"`
	Hash     string   `json:"hash,omitempty"`
	Action   string   `json:"action"` // "put" | "delete"
}

type Freshness int

const (
	Shrug Freshness = iota
	KnownStale
	ProbablyFresh
	Fresh
)

// Classify implements the read-path decision table in §6 as a pure function.
func Classify(local, known *Revision, tomb *Revision, wantAtLeast *Revision) Freshness {
	if tomb != nil && wantAtLeast != nil && !wantAtLeast.Less(*tomb) {
		return Fresh // deleted, and caller's floor is already covered by the tombstone
	}
	if local == nil {
		return Shrug
	}
	if known == nil {
		return ProbablyFresh
	}
	if local.Less(*known) {
		return KnownStale
	}
	return Fresh
}

// Store wraps the Redis key namespace in §16. A real implementation backs this
// with go-redis/v9 (as logma already does) against a single standalone instance.
type Store interface {
	Head(ctx context.Context, key string) (*Revision, error)
	Known(ctx context.Context, key string) (*Revision, error)
	Tombstone(ctx context.Context, key string) (*Revision, error)

	// AdvanceKnown atomically applies max(current, incoming) via the Lua script
	// in §5. Returns whether it actually advanced (for metrics/dedup).
	AdvanceKnown(ctx context.Context, key string, incoming Revision) (advanced bool, err error)

	PutObject(ctx context.Context, meta ObjectMeta, body []byte) error
	GetObject(ctx context.Context, key string, rev Revision) (ObjectMeta, []byte, error)
	SetHead(ctx context.Context, key string, rev Revision) error
	SetTombstone(ctx context.Context, key string, rev Revision) error

	// AcquireFetchLease implements the SET NX PX stampede guard in §9.
	AcquireFetchLease(ctx context.Context, key string, ttl time.Duration) (acquired bool, err error)
}

// VerifyContent enforces §8: integrity checking is mandatory, not advisory.
func VerifyContent(meta ObjectMeta, body []byte) error {
	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])
	if got != meta.ContentHash {
		return fmt.Errorf("nqc: content hash mismatch for %s@%s: want %s got %s",
			meta.Key, meta.Revision, meta.ContentHash, got)
	}
	return nil
}

// Subscriber consumes nqc:update hints and applies the monotonic merge (§5).
type Subscriber struct {
	store Store
}

func (s *Subscriber) HandleHint(ctx context.Context, h Hint) error {
	switch h.Action {
	case "delete":
		if err := s.store.SetTombstone(ctx, h.Key, h.Revision); err != nil {
			return fmt.Errorf("nqc: tombstone %s: %w", h.Key, err)
		}
	case "put":
		if _, err := s.store.AdvanceKnown(ctx, h.Key, h.Revision); err != nil {
			return fmt.Errorf("nqc: advance known %s: %w", h.Key, err)
		}
	default:
		return fmt.Errorf("nqc: unknown hint action %q for %s", h.Action, h.Key)
	}
	return nil
}

// ShardDigest is what reconciliation compares against the origin's manifest (§10).
type ShardDigest struct {
	Shard  string
	Digest string // sha256 over sorted {key: revision} pairs in the shard
}

// Reconciler is the correctness backstop: periodic, bounded-interval repair that
// does not depend on Pub/Sub having delivered anything.
type Reconciler struct {
	store     Store
	origin    OriginManifest
	Interval  time.Duration
}

type OriginManifest interface {
	ShardDigest(ctx context.Context, shard string) (ShardDigest, error)
	ShardEntries(ctx context.Context, shard string) (map[string]Revision, error)
}

func (r *Reconciler) ReconcileShard(ctx context.Context, shard string, localDigest ShardDigest) error {
	remote, err := r.origin.ShardDigest(ctx, shard)
	if err != nil {
		return fmt.Errorf("nqc: fetch manifest digest for shard %s: %w", shard, err)
	}
	if remote.Digest == localDigest.Digest {
		return nil // converged, nothing to do
	}
	entries, err := r.origin.ShardEntries(ctx, shard)
	if err != nil {
		return fmt.Errorf("nqc: fetch shard entries for shard %s: %w", shard, err)
	}
	for key, rev := range entries {
		if _, err := r.store.AdvanceKnown(ctx, key, rev); err != nil {
			return fmt.Errorf("nqc: reconcile advance %s: %w", key, err)
		}
	}
	return nil
}

// Origin-side publish: write-then-publish ordering from §5. The durable write
// must succeed before the hint goes out, never the reverse.
type Origin struct {
	store   DurableStore
	notify  func(ctx context.Context, h Hint) error
}

type DurableStore interface {
	Put(ctx context.Context, key string, body []byte, contentType string) (Revision, string /*hash*/, error)
	Delete(ctx context.Context, key string) (Revision, error)
}

func (o *Origin) Put(ctx context.Context, key string, body []byte, contentType string) error {
	rev, hash, err := o.store.Put(ctx, key, body, contentType)
	if err != nil {
		return fmt.Errorf("nqc: durable write %s: %w", key, err)
	}
	return o.notify(ctx, Hint{Key: key, Revision: rev, Hash: hash, Action: "put"})
}

func (o *Origin) Delete(ctx context.Context, key string) error {
	rev, err := o.store.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("nqc: durable delete %s: %w", key, err)
	}
	return o.notify(ctx, Hint{Key: key, Revision: rev, Action: "delete"})
}
```

## 18. Open questions if this were actually built

- Manifest digest algorithm and shard count — sha256 over a sorted key list is simple but
  not incremental; a Merkle tree would make partial shard updates cheaper at the cost of
  real complexity. Start with the flat digest (§10) and only go to a tree if shard sizes
  force it.
- ACL model for `nqc:*` PUBLISH restriction (§12) — decide whether this rides on Redis 6+
  ACLs directly or needs an app-level HMAC given the org's existing auth patterns.
- Metrics backend/labels for §15 — align with whatever `logma` already emits to, if
  anything, rather than introducing a new one.
- Whether `NQC-2` ("Ask Mom") is worth the round-trip in practice versus just calling it
  "Actual consistency" with a short TTL — needs real latency data, not guessing.
- Multi-writer HLC mode (§13) is speculative; don't build it until a second writer is an
  actual requirement.

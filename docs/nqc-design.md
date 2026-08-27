# Not Quite Consistency (NQC): a Redis Pub/Sub CDN protocol

Status: design only, not implemented.

NQC is a protocol for using independent standalone Redis OSS instances as regional or
edge cache islands. Redis Pub/Sub is the low-latency hint path. It is deliberately not
the durable log, the replication mechanism, or the source of truth.

The central rule is:

> Missing a hint is acceptable. Being unable to discover and repair the miss is not.

A useful implementation should preserve that rule through subscriber disconnects, Redis
restarts, origin failures, duplicate and reordered messages, partial fanout, concurrent
refreshes, deletes, cache eviction, and process crashes.

## 1. Goals and non-goals

NQC aims to provide:

- fast invalidation/update hints through Redis Pub/Sub
- independent Redis cache islands with no Redis Cluster or Redis replication dependency
- immutable cached object revisions
- monotonic, authority-confirmed convergence
- lazy refresh and stale-while-revalidate
- bounded background work and stampede protection
- periodic anti-entropy that repairs missed hints
- explicit degraded states rather than pretending a cache is current when it cannot know

NQC does not provide:

- consensus or linearizable reads
- a durable event stream
- cross-Redis Pub/Sub
- hostile-tenant isolation merely by using Redis logical DBs
- guaranteed freshness during a network partition
- a replacement for the authoritative origin

The convergence guarantee applies to authority-confirmed control state first. Cached bytes
are refreshed according to policy and demand.

## 2. Topology: Pub/Sub is local to each Redis island

A standalone Redis server's Pub/Sub messages do not propagate to another standalone Redis
server. A robust NQC topology must explicitly say who fans hints out.

~~~text
                         authoritative origin
                                  |
                       commit object/state rev=42
                                  |
                        best-effort hint fanout
                     /             |              \
                    v              v               v
             +-------------+ +-------------+ +-------------+
             | island A    | | island B    | | island C    |
             |             | |             | |             |
             | edge agent  | | edge agent  | | edge agent  |
             |      |      | |      |      | |      |      |
             | Redis A     | | Redis B     | | Redis C     |
             | Pub/Sub     | | Pub/Sub     | | Pub/Sub     |
             | + cache     | | + cache     | | + cache     |
             +-------------+ +-------------+ +-------------+
                    \              |               /
                     \------ origin manifest -----/
                           anti-entropy repair
~~~

The origin or a stateless NQC notifier opens a connection to each island and performs the
same local PUBLISH. Fanout is best-effort:

- a failed publish to island B does not roll back the authoritative mutation
- PUBLISH returning zero subscribers is an observability signal, not a correctness ACK
- notification retries may be bounded and opportunistic
- anti-entropy is responsible for repairing every missed notification

A committed write must never be reported as uncommitted merely because hint fanout failed.
Otherwise a caller may retry the mutation and accidentally issue a second revision.

An optional relay layer may perform fanout, but the relay is still not a durable log. If
it misses or drops a notification, reconciliation repairs the gap.

## 3. Relationship to logma

The protocol mirrors useful patterns from sibling services such as logma: small typed
events, bounded workers, monotonic state updates, Redis Pub/Sub as transient transport,
explicit catch-up, internal implementation packages, and ordinary environment-based
configuration.

logma-nqc should remain independent rather than importing logma/internal packages. A CDN
cache and an event relay have different failure semantics even when some primitives look
similar.

## 4. Cache identity and namespaces

A CDN cache key must be more precise than a URL path. A shared cache that ignores
namespace, query semantics, content negotiation, or authorization can alias or leak data.

Every object belongs to a stable namespace:

~~~text
namespace + canonical_cache_key -> key_id
~~~

Recommended key ID:

~~~text
key_id = SHA-256(namespace || 0x00 || canonical_cache_key)
~~~

Use the lowercase hexadecimal digest in Redis keys. Store the human-readable canonical
key only as metadata where needed.

The canonical cache key is adapter-specific, but an HTTP adapter normally needs to account
for:

- host/origin identity
- normalized path
- query parameters the origin declares significant
- representation dimensions such as content encoding
- allowed Vary dimensions
- tenant/project namespace when the cache is not public

Do not cache authenticated or user-specific responses in a shared namespace unless that
behavior is explicitly designed and keyed.

Namespaces interpolated into Redis key/channel patterns should use a safe grammar such as
[a-z0-9._-]{1,64}. Do not place arbitrary tenant input directly into ACL patterns.

Redis logical DBs may be useful organizationally, but they are not a hostile-tenant
security boundary and do not isolate maxmemory eviction.

## 5. Revisions: durable, monotonic, and wire-safe

All mutations in an NQC namespace use one totally ordered revision domain. Revisions must
never be reused for different authoritative states and must never move backward.

A practical representation is:

~~~text
revision = <era:uint64><seq:uint64>
wire     = 32 lowercase hexadecimal characters
~~~

The fixed-width wire representation makes lexical byte ordering match tuple ordering. It
is also represented as a JSON string, avoiding JavaScript's 53-bit integer precision
limit.

Rules:

- seq increments durably for each committed mutation
- ordinary process restarts resume the durable sequence
- era changes only when allocator state is intentionally reset/restored in a way that
  could otherwise reuse an old sequence
- era itself must be durable and monotonic
- timestamps never order mutations

If the authoritative system cannot provide a non-reusing monotonic revision allocator,
NQC cannot safely claim monotonic convergence.

## 6. Authoritative commit point

The authoritative mutation and manifest state need one well-defined commit point.
Publishing a hint is always after that point.

For a put:

~~~text
1. allocate candidate revision R
2. write immutable body/blob for R
3. atomically commit current-state metadata for key -> PUT R
   plus the corresponding manifest/shard metadata
4. publish PUT R to each Redis island, best effort
~~~

For a delete:

~~~text
1. allocate candidate revision R
2. atomically commit current-state metadata for key -> DELETE R
   plus the corresponding manifest/shard metadata
3. publish DELETE R to each Redis island, best effort
~~~

If the body write succeeds but the metadata commit fails, the body is an orphan and may be
garbage-collected. It was never authoritative.

An object store without general transactions should use a small authoritative metadata
object/table as the commit record and update it with the store's conditional-write
primitive. The large body remains immutable and referenced by that record.

A refresh does not need the exact revision named in a hint. The origin may return any
current authoritative revision greater than or equal to the requested floor. This lets an
edge jump directly from 100 to 105.

## 7. Redis state model

The earlier design used separate known and tomb keys. That creates conflicting partial
truths and makes every path responsible for resolving ordering between them.

NQC v1 instead models deletion as another versioned state.

~~~text
nqc:data:<ns>:<key_id>:<rev>       immutable cached bytes + metadata; expiring
nqc:head:<ns>:<key_id>             newest locally installed live revision
nqc:confirmed:<ns>:<key_id>        newest authority-confirmed state: PUT or DELETE
nqc:hint:<ns>:<key_id>             newest Pub/Sub hint; soft state with TTL
nqc:floor:<ns>:<shard>             completed-snapshot high-watermark
nqc:index:<ns>:<shard>             bounded local index of confirmed key IDs
nqc:fetch:<ns>:<key_id>            short refresh lease
~~~

A confirmed state is conceptually:

~~~text
{
  revision,
  kind: PUT | DELETE,
  content_hash,     // PUT only
  metadata...
}
~~~

Three invariants matter:

1. confirmed advances only from an authenticated authoritative read or a verified
   manifest snapshot.
2. hint is only a soft claim. A broken publisher must not permanently wedge the edge with
   an impossible high revision.
3. head never moves backward and never becomes visible before its bytes pass integrity
   checks.

All state-advance operations are atomic max-by-revision operations. Plain SET head R or a
plain SET delete R is not sufficient.

## 8. Hint wire protocol

Use a versioned, bounded message format.

~~~json
{
  "v": 1,
  "namespace": "public-assets",
  "key": "/foo.json",
  "key_id": "9b7e4f0d64c2a9e74b16c93d90b01d4c46bb1d67f5d7e9f30ad6ad27f977e4aa",
  "revision": "0000000000000001000000000000006b",
  "op": "put",
  "content_hash": "sha256:abc..."
}
~~~

For deletes, omit content_hash.

Subscriber validation rejects or ignores:

- unknown protocol versions
- malformed namespace/key IDs
- non-canonical revision strings
- unknown operations
- key strings whose canonicalization does not reproduce key_id
- oversized keys/messages
- malformed digest values

A hint must never contain an arbitrary origin URL that the edge blindly fetches. The edge
resolves a validated cache identity through its configured trusted origin, preventing
Pub/Sub from becoming an SSRF control plane.

### Soft hints versus confirmed state

On receipt:

~~~text
hint(key) = max(hint(key), incoming)
~~~

but only in the soft hint store. Hints have a bounded TTL. AdvanceHint must also consult
the shard floor: when a key has no confirmed live entry and incoming.revision is less
than or equal to the completed snapshot floor, ignore the hint as already covered by
anti-entropy. This is the concrete rule that prevents an old delayed PUT from resurrecting
a key after its per-key delete state has been garbage-collected.

If a hint claims revision 999 while authoritative revalidation says the current state is
100, the edge records a suspect-hint metric and does not permanently advance confirmed to
999. The bad hint may be cleared after authoritative confirmation or simply expire.

This matters for both compromise and operational mistakes. Monotonic state is only useful
if the inputs being made monotonic are trustworthy.

The hash carried by a hint is useful for coalescing and diagnostics. Bytes are ultimately
verified against hash metadata obtained from the authoritative origin or manifest, not
merely against a Pub/Sub field.

## 9. Pub/Sub ingestion and backpressure

The Pub/Sub receive loop stays cheap:

~~~text
decode -> validate -> atomically advance soft hint -> enqueue/coalesce optional work
~~~

It must not perform origin requests inline.

Use a bounded worker pool for push-through or background refresh. If the queue is full,
dropping a refresh job is safe because:

- the soft hint remains present
- the next request can refresh
- reconciliation eventually repairs state

Coalesce repeated work by key and prefer the newest revision. A burst of 101, 102, 103,
104 should result in work for 104 rather than four origin fetches.

On subscriber startup and after a detected reconnect, schedule a jittered reconciliation.
Do not assume reconnect can replay missed Pub/Sub messages.

## 10. Read path

A read considers four independent inputs:

- locally installed bytes in head
- authority-confirmed state
- newer soft hints
- local age/TTL policy

Confirmed deletes always take precedence over older local bytes.

~~~text
confirmed=DELETE >= head            -> DELETED; never serve older head
head missing, confirmed=PUT         -> REFRESH_REQUIRED
head missing, confirmed=DELETE      -> DELETED
head missing, newer hint exists     -> REFRESH_REQUIRED / confirm
head missing, no useful state       -> SHRUG / origin lookup

head == confirmed PUT,
no newer hint,
object within freshness age         -> FRESH

head < confirmed PUT                -> KNOWN_STALE
head <= newer hint                  -> SUSPECTED_STALE
head exists, confirmed missing      -> PROBABLY_FRESH
head > confirmed                    -> invariant drift; SHRUG + repair
~~~

A soft delete hint should trigger confirmation immediately under the default policy and
may suppress stale serving when deletion is treated as revocation. NQC does not promise
revocation semantics for its most permissive tier; security-sensitive revocation should
use authoritative validation.

Object age is separate from revision freshness. An object can be the newest known revision
and still exceed its freshness TTL.

~~~text
fresh_until     ordinary cache freshness
stale_until     maximum stale-if-error window
data_key_ttl    >= stale_until, so allowed stale bytes still exist
~~~

Never serve corrupted bytes or bytes beyond the configured maximum stale window.

## 11. Refresh and stampede control

Refresh is per island and per key.

Acquire a short lease:

~~~text
SET nqc:fetch:<ns>:<key_id> <random-token> NX PX <ttl>
~~~

The winner:

1. re-reads the newest confirmed/hinted floor
2. resolves the key against the authoritative origin
3. accepts a current revision greater than or equal to that floor
4. verifies authoritative metadata and content hash
5. writes the immutable data key
6. atomically advances confirmed and head if the returned revision still wins
7. lets waiters observe the new head

Losers either serve allowed stale bytes or wait for a small bounded interval with jitter
and retry the state read.

Lease release must compare the random token before deleting the lease. A late worker must
not delete a lease acquired by a newer worker. If fetch duration may exceed the lease TTL,
use a sufficiently bounded TTL or renew only while ownership is still proven.

A fetch for revision 107 that completes after 108 was installed must not move head
backward. Monotonic head advancement is mandatory.

Use timeouts, bounded concurrency, retry backoff, and an origin circuit breaker. NQC's
fallback path must not become an origin denial-of-service amplifier.

## 12. Deletes and the no-resurrection rule

Deletes are ordered states.

~~~text
PUT    rev=108
DELETE rev=110
PUT    rev=113   // legitimate recreation
~~~

Because they share one revision order:

- old PUT 108 cannot beat DELETE 110
- old DELETE 110 cannot beat PUT 113
- recreation is valid when it has a newer revision

A delete hint advances only soft state. An authoritative resolve or reconciliation
advances confirmed to DELETE.

Per-key delete records cannot grow forever on a high-churn namespace. Complete manifest
snapshots and shard floors make bounded tombstone retention safe.

If the origin cannot provide a complete enumerable manifest, the stronger no-resurrection
guarantee after arbitrarily long offline periods is unavailable. In that mode, tombstones
must be retained for at least the maximum supported offline/reconciliation interval, or
indefinitely if no such bound exists.

## 13. Anti-entropy: the correctness backstop

Anti-entropy is what makes NQC more than Pub/Sub invalidation.

The authoritative namespace is divided using a fixed shard scheme.

~~~text
shard = first K bits of key_id
~~~

Shard count and hashing scheme are protocol configuration. They cannot silently change;
changing them requires a manifest-version or namespace migration.

Each shard exposes cheap metadata:

~~~text
generation
digest
last_revision
~~~

and a complete immutable snapshot identified by a snapshot token:

~~~text
{
  namespace,
  shard,
  snapshot_token,
  generation,
  high_watermark,
  digest,
  entries: [
    { key_id, canonical_key, revision, content_hash, metadata... },
    ...
  ]
}
~~~

Entries are the authoritative live set as of high_watermark. Deleted keys are represented
by absence from the complete snapshot; transient delete entries may additionally exist for
diagnostics or incremental APIs.

### Snapshot coherence

Pagination is pinned to one snapshot token/generation. Page 1 from generation 20 and page
2 from generation 21 are not a valid snapshot.

A reconciler:

1. fetches shard metadata
2. skips the full snapshot when a previously verified generation/digest matches
3. otherwise pages one immutable snapshot
4. verifies the snapshot digest
5. applies every live entry monotonically to confirmed
6. compares the complete snapshot with the local per-shard index and invalidates local
   keys absent from the snapshot
7. only after steps 3-6 succeed, advances nqc:floor:<ns>:<shard> to high_watermark

If the origin cannot pin a snapshot and it changes during pagination, restart that shard
with jitter instead of accepting a mixed view.

### Canonical digest

Do not hash ad-hoc JSON maps. Define a canonical byte encoding over entries sorted by
key_id, including at least key_id, revision, content_hash, and cache-relevant metadata.
Length-prefix variable fields before hashing.

SHA-256 is sufficient for change detection. The digest is not an authentication
mechanism.

### Shard floors and long-offline repair

After a complete snapshot at high-watermark W, every mutation at revision <= W for that
shard is represented by either:

- a live confirmed entry from the snapshot
- the key being absent

The edge stores floor=W. A stale hint for an absent key with revision <= W cannot
resurrect it. A legitimate recreation has revision > W.

This lets old per-key delete state be garbage-collected while still allowing a long-offline
edge to recover safely.

The invariant depends on control state not being silently evicted after snapshot
completion.

### No server-wide discovery scan

Do not make server-wide SCAN or pattern discovery the protocol index.

Maintain an explicit per-shard index, for example:

~~~text
ZADD nqc:index:<ns>:<shard> 0 <key_id>
~~~

Update it atomically with confirmed-state changes and enumerate it in bounded lexical
pages. Reconciliation remains scoped to NQC-owned state with predictable work.

## 14. Reconciliation scheduling

Run reconciliation:

- on edge startup
- after Redis state loss/restart
- after Pub/Sub reconnect
- periodically with jitter
- opportunistically after repeated suspect hints or origin mismatches

Jitter both start time and per-shard work. Bound concurrent shard fetches and origin
requests.

A six-hour reconciliation interval is only a scheduling target. NQC can bound
time-to-attempt, not time-to-success through a network or origin outage.

## 15. Redis memory, eviction, and persistence

Control keys are small and correctness-relevant while the edge is running. Data keys are
large and disposable.

Redis maxmemory is server-wide across logical DBs. Putting control keys in DB 0 and object
bytes in DB 1 does not isolate eviction.

Two sane profiles are:

### One Redis process

- all cached data keys have TTLs
- control keys do not expire
- use a volatile-only policy such as volatile-lfu so only expiring cache data is eligible
  for eviction
- keep control state bounded with shard floors, index cleanup, and hint TTLs
- reserve headroom so control writes do not hit OOM after all evictable data is gone

### Separate control/data Redis processes

Use this when losing control state under memory pressure is unacceptable. Separate Redis
processes provide actual memory-policy isolation; logical DBs do not.

Redis persistence is optional because NQC state is derived from the authority. With
persistence disabled, Redis restart means cold cache and must trigger bootstrap
reconciliation before authority-confirmed freshness is claimed.

Large Redis values can stall a standalone server. Set a maximum cacheable object size and
bypass Redis for oversized objects. Prefer asynchronous/lazy freeing such as UNLINK and
appropriate lazyfree settings when retiring large values.

Old immutable revisions expire. Keep enough history for configured stale serving; do not
accumulate every revision forever.

## 16. HTTP CDN adapter requirements

NQC is a cache protocol, not permission to ignore HTTP caching rules.

An HTTP adapter should at minimum:

- honor Cache-Control: no-store
- avoid shared caching of private responses
- treat Vary: * as uncacheable
- include approved Vary dimensions in the canonical cache key
- define query normalization explicitly
- avoid caching Authorization or user-cookie responses unless using an intentionally
  private namespace
- preserve content-encoding representation identity
- cap body and metadata sizes
- validate origin response status before installing bytes

Origin ETags may be retained for HTTP conditional requests, but NQC's SHA-256 digest is a
separate integrity field.

## 17. Security and Redis ACLs

Use private networking/TLS as appropriate and Redis ACLs for least-privilege separation.

Conceptual roles per island:

~~~text
publisher
  PUBLISH only to nqc:v1:<namespace>:update
  no cache key reads/writes required

subscriber/edge
  SUBSCRIBE to its NQC update channel
  access only to its nqc:<namespace>:* keyspace
  no PUBLISH unless explicitly required

operator
  administrative access, separate identity
~~~

Redis ACL channel patterns and key patterns are separate controls. Do not grant PUBSUB
CHANNELS, PUBLISH, KEYS, FLUSHALL, or broad administrative command categories merely
because a subscriber needs SUBSCRIBE.

If hostile tenants share one Redis process, logical DB numbers are not sufficient
isolation. Use distinct ACL identities and namespaces at minimum; separate Redis processes
when the risk boundary requires it.

A compromised publisher can cause denial-of-freshness by emitting plausible bad hints.
Soft-hint TTL and authoritative confirmation prevent that from permanently poisoning
confirmed state.

## 18. Consistency contract

NQC exposes policy tiers rather than a binary consistent/inconsistent claim.

~~~text
NQC-0  "¯\_(ツ)_/¯"
       Serve locally valid cached bytes without a remote freshness check.
       Still honor locally confirmed deletes, corruption checks, and maximum stale age.
       Best for immutable/fingerprinted assets.

NQC-1  "Probably"
       Default. Serve local unless confirmed state or a newer soft hint says refresh.
       Pub/Sub gives low-latency invalidation; anti-entropy repairs misses.

NQC-2  "Ask Mom"
       Revalidate authoritative metadata/head synchronously, then serve matching cached
       bytes or refresh. This is revalidation, not linearizability: the origin can commit
       another write immediately after validation unless it provides a stronger
       conditional-read contract.

ORIGIN
       Bypass NQC and use the authoritative origin's own consistency contract.
~~~

Useful state labels are:

~~~text
FRESH
PROBABLY_FRESH
KNOWN_STALE
SUSPECTED_STALE
SHRUG
DELETED
~~~

DELETED is a resolution outcome, not freshness. Do not overload a freshness enum to mean a
404.

### Formal-ish guarantee

Assume:

1. one authoritative revision order does not regress
2. the namespace eventually stops changing long enough to observe
3. an edge continues reconciliation attempts
4. communication with the authority eventually succeeds
5. a reconciliation snapshot is complete and coherent

Then the edge's confirmed control state eventually equals the authoritative snapshot.

For NQC-1, the next successful refresh/read of a stale live object eventually installs the
current body. NQC-0 deliberately makes a weaker serving promise and may keep serving an
allowed stale body until local policy forces refresh.

During a partition, no finite freshness bound is claimed.

Operationally: everyone eventually agrees, unless they do not; if communication recovers,
they can prove what they missed and repair it.

## 19. Failure behavior

### Origin commits, all Pub/Sub fanout fails

The mutation is committed. Return mutation success according to origin semantics, emit
fanout-failure metrics, and rely on reconciliation.

### Edge misses many hints

No replay is required. The next manifest snapshot advances directly to current state.

### Redis restarts with no persistence

Treat it as a cold cache. Do not infer freshness from absent control keys. Bootstrap
reconciliation and fetch on demand.

### Older refresh finishes after newer refresh

The older body may remain as immutable cache history, but monotonic head advancement
prevents regression.

### Delete then recreate

The newer PUT wins naturally by revision.

### Bogus high hint

It may trigger refresh work but cannot permanently advance confirmed. Confirm against the
authority, quarantine/expire the soft hint, and emit a metric.

### Manifest changes during pagination

Reject the mixed snapshot and retry with jitter.

### Origin unavailable while stale bytes exist

Serve only if policy allows and the object is within stale_until; otherwise fail closed
for freshness.

## 20. Observability is part of the protocol

Track at least:

- Pub/Sub fanout failures per island
- local PUBLISH zero-subscriber counts
- subscriber reconnects
- hint validation failures
- suspect/bogus-high hint confirmations
- hint-to-confirmed lag
- SHRUG rate
- stale serves by policy tier
- stale age at serve time
- reconciliation attempts/failures/duration
- shard generation/digest mismatches
- snapshot restarts caused by generation changes
- time since each shard last completed a verified snapshot
- keys invalidated because absent from a complete snapshot
- fetch lease contention and lease expiry
- origin fetch latency/error rate
- Redis evictions/OOMs
- content-hash mismatches

Do not use raw cache keys as metric labels. Label by namespace, shard, policy, outcome, and
a bounded key class to avoid unbounded metric cardinality.

## 21. Illustrative Go shapes

These are design sketches, not a package promised to compile as-is.

~~~go
package nqc

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "time"
)

type Revision struct {
    Era uint64
    Seq uint64
}

func (r Revision) Compare(o Revision) int {
    switch {
    case r.Era < o.Era:
        return -1
    case r.Era > o.Era:
        return 1
    case r.Seq < o.Seq:
        return -1
    case r.Seq > o.Seq:
        return 1
    default:
        return 0
    }
}

func (r Revision) Wire() string {
    return fmt.Sprintf("%016x%016x", r.Era, r.Seq)
}

type StateKind uint8

const (
    StatePut StateKind = iota + 1
    StateDelete
)

type AuthorityState struct {
    Revision    Revision
    Kind        StateKind
    ContentHash string
    ContentType string
    ETag        string
    FreshUntil  time.Time
    StaleUntil  time.Time
}

type Object struct {
    State AuthorityState
    Body  []byte
}

type Store interface {
    Head(context.Context, string, string) (*AuthorityState, error)
    Confirmed(context.Context, string, string) (*AuthorityState, error)
    Hint(context.Context, string, string) (*AuthorityState, error)

    AdvanceHint(context.Context, string, string, AuthorityState, time.Duration) (bool, error)
    InstallPut(context.Context, string, string, Object) error
    ConfirmDelete(context.Context, string, string, AuthorityState) error

    ShardFloor(context.Context, string, string) (*Revision, error)
    CommitShardFloor(context.Context, string, string, Revision) error

    AcquireFetchLease(context.Context, string, string, string, time.Duration) (bool, error)
    ReleaseFetchLease(context.Context, string, string, string) error
}

type Origin interface {
    Resolve(ctx context.Context, namespace, key string, min *Revision) (Object, error)
    ShardMeta(ctx context.Context, namespace, shard string) (ShardMeta, error)
    ShardSnapshot(ctx context.Context, namespace, shard, token, cursor string, count int) (SnapshotPage, error)
}

type ShardMeta struct {
    Shard         string
    Generation    string
    SnapshotToken string
    Digest        string
    HighWatermark Revision
}

type ManifestEntry struct {
    Key         string
    KeyID       string
    Revision    Revision
    ContentHash string
}

type SnapshotPage struct {
    Meta       ShardMeta
    Entries    []ManifestEntry
    NextCursor string
}

func VerifyContent(hash string, body []byte) error {
    const prefix = "sha256:"
    if len(hash) <= len(prefix) || hash[:len(prefix)] != prefix {
        return fmt.Errorf("nqc: invalid content hash")
    }
    want := hash[len(prefix):]
    sum := sha256.Sum256(body)
    got := hex.EncodeToString(sum[:])
    if got != want {
        return fmt.Errorf("nqc: content hash mismatch: want %s got %s", want, got)
    }
    return nil
}
~~~

The Redis implementation should concentrate monotonic pointer updates, lease release,
shard-index maintenance, and floor advancement into small audited Lua or Redis Function
operations rather than scattering multi-command races across request handlers.

The object body should not be processed by a long-running Lua loop. Validate it in the
application, write the immutable value with one bounded Redis command, then use a small
atomic control-plane operation to publish the pointer.

## 22. Required failure-injection tests

Before calling an implementation production-ready, test at least:

- hints delivered in order, reversed, duplicated, and randomly dropped
- disconnect/reconnect while updates occur
- partial fanout where only one island receives a hint
- edge Redis restart with persistence disabled
- crash after immutable body write but before head advancement
- concurrent refreshes where 107 finishes after 108
- lease expiry followed by a late owner attempting release
- delete followed by stale PUT delivery
- delete followed by legitimate recreation
- bogus high revision hint
- malformed namespace/key/revision/hash hint
- a revision above JavaScript's exact integer range on the wire
- manifest generation change halfway through pagination
- long-offline edge after old delete records have been garbage-collected
- cache-key canonicalization collisions and HTTP Vary cases
- maxmemory pressure proving control keys are not eligible for volatile eviction
- oversized object bypass
- corrupted origin/cache body hash
- full worker queue proving dropped refresh work is later repaired
- origin unavailable with and without stale bytes inside the allowed stale window

Property tests are especially useful for the core invariant:

> Applying any permutation with duplicates of states from a totally ordered revision
> history must never make confirmed or head state move backward.

## 23. Open questions

Keep these as implementation choices until measurements justify more machinery:

- shard count and snapshot page size
- flat SHA-256 shard digest versus a Merkle structure
- exact maximum cacheable object size
- refresh worker count and per-origin concurrency budget
- whether NQC-2's metadata round-trip is useful enough to expose publicly
- exact HTTP cache-key policy for each deployment
- whether a non-enumerable origin is acceptable with weaker delete-GC guarantees

Multi-writer conflict resolution is intentionally not part of NQC v1. If a second
authoritative writer becomes a real requirement, prefer preserving one shared revision
allocator. HLC/LWW conflict resolution is a separate distributed-write problem and should
not be smuggled into the CDN protocol as an optional footnote.

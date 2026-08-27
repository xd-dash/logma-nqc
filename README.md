# logma-nqc

Not Quite Consistency (NQC) is a Redis Pub/Sub CDN/cache protocol for independent,
standalone Redis OSS instances. Pub/Sub is the low-latency hint path; an authoritative
origin plus sharded anti-entropy is the correctness backstop.

The protocol is documented in [`docs/nqc-design.md`](docs/nqc-design.md). The Go reference
implementation lives in [`nqc`](nqc).

## Core model

- each Redis instance is an independent cache island
- the origin/notifier publishes the same hint independently to each island
- Pub/Sub messages only advance TTL'd **soft hint** state
- only authoritative origin reads or verified manifest snapshots advance **confirmed** state
- cached object revisions are immutable and heads only move forward
- deletes and puts share one total revision order
- complete shard snapshots plus shard high-watermarks repair missed Pub/Sub messages
- no Redis Cluster, Redis replication, Streams, or server-wide `SCAN` is required

## Minimal usage

```go
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

store, err := nqc.NewRedisStore(client, nqc.StoreConfig{
    Namespace: "public-assets",
    ShardBits: 8,
})
if err != nil {
    log.Fatal(err)
}

subscriber, err := nqc.NewSubscriber(client, store, nqc.SubscriberConfig{}, nqc.SubscriberHooks{})
if err != nil {
    log.Fatal(err)
}
go func() {
    if err := subscriber.Run(ctx); err != nil {
        log.Printf("nqc subscriber: %v", err)
    }
}()

cache, err := nqc.NewCache(store, origin, nqc.CacheConfig{})
if err != nil {
    log.Fatal(err)
}

result, err := cache.Get(ctx, "/app.js", nqc.NQC1)
```

`origin` implements `nqc.Origin`; it owns the authoritative current object state and the
immutable, paginated shard-manifest snapshots used by reconciliation.

Publish a committed mutation to one or more independent Redis islands with
`FanoutPublisher`:

```go
hint, err := nqc.NewHint("public-assets", "/app.js", committedState)
if err != nil {
    log.Fatal(err)
}

results := (&nqc.FanoutPublisher{
    Namespace: "public-assets",
    Islands: []nqc.Island{
        {Name: "us-west", Client: westRedis},
        {Name: "us-east", Client: eastRedis},
    },
}).Publish(ctx, hint)
```

A publish failure or zero-subscriber result is observable but does not roll back the
origin mutation. `Reconciler` repairs the missed state later.

## Tests

Unit tests run without Redis. Integration tests run when `NQC_REDIS_ADDR` is set:

```sh
NQC_REDIS_ADDR=127.0.0.1:6379 go test -race ./...
```

The repository CI starts one standalone Redis OSS service and runs the integration suite.

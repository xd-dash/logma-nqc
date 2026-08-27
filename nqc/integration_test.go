package nqc

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testStore(t *testing.T) (*redis.Client, *RedisStore) {
	t.Helper()
	addr := os.Getenv("NQC_REDIS_ADDR")
	if addr == "" {
		t.Skip("NQC_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	store, err := NewRedisStore(client, StoreConfig{Namespace: "test", ShardBits: 8})
	if err != nil {
		t.Fatal(err)
	}
	return client, store
}

func putObject(rev uint64, body string) Object {
	now := time.Now().UTC()
	b := []byte(body)
	return Object{State: AuthorityState{
		Revision: Revision{Era: 1, Seq: rev}, Kind: StatePut, ContentHash: HashContent(b),
		ContentType: "text/plain", ETag: `"v"`, FreshUntil: now.Add(time.Minute), StaleUntil: now.Add(time.Hour),
	}, Body: b}
}

func TestRedisStoreNeverMovesHeadBackward(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	if _, err := store.InstallPut(ctx, "/x", putObject(2, "two")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InstallPut(ctx, "/x", putObject(1, "one")); err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(ctx, "/x")
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.State.Revision.Seq != 2 || string(head.Body) != "two" {
		t.Fatalf("head regressed: %#v", head)
	}
}

func TestDeleteThenRecreate(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	if _, err := store.InstallPut(ctx, "/x", putObject(1, "one")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConfirmDelete(ctx, "/x", AuthorityState{Revision: Revision{Era: 1, Seq: 2}, Kind: StateDelete}); err != nil {
		t.Fatal(err)
	}
	if head, err := store.Head(ctx, "/x"); err != nil || head != nil {
		t.Fatalf("delete did not suppress head: head=%#v err=%v", head, err)
	}
	if _, err := store.InstallPut(ctx, "/x", putObject(3, "three")); err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(ctx, "/x")
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.State.Revision.Seq != 3 {
		t.Fatalf("recreate failed: %#v", head)
	}
}

func TestShardFloorRejectsOldHintForAbsentKey(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	keyID, err := KeyID("test", "/gone")
	if err != nil {
		t.Fatal(err)
	}
	shard, err := ShardForKeyID(keyID, store.ShardBits())
	if err != nil {
		t.Fatal(err)
	}
	floor := Revision{Era: 1, Seq: 10}
	if err := store.CommitShardState(ctx, shard, ShardState{Generation: "g1", Digest: HashContent(nil), SnapshotToken: "s1", Floor: floor}); err != nil {
		t.Fatal(err)
	}
	advanced, err := store.AdvanceHint(ctx, "/gone", AuthorityState{
		Revision: Revision{Era: 1, Seq: 9}, Kind: StatePut, ContentHash: HashContent([]byte("old")),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if advanced {
		t.Fatal("old hint advanced past completed shard floor")
	}
	if hint, err := store.Hint(ctx, "/gone"); err != nil || hint != nil {
		t.Fatalf("old hint persisted: hint=%#v err=%v", hint, err)
	}
}

func TestShardFloorRejectsConflictingMetadataAtSameRevision(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	keyID, err := KeyID("test", "/x")
	if err != nil {
		t.Fatal(err)
	}
	shard, err := ShardForKeyID(keyID, store.ShardBits())
	if err != nil {
		t.Fatal(err)
	}
	floor := Revision{Era: 1, Seq: 10}
	first := ShardState{Generation: "g1", Digest: HashContent([]byte("one")), SnapshotToken: "s1", Floor: floor}
	if err := store.CommitShardState(ctx, shard, first); err != nil {
		t.Fatal(err)
	}
	conflict := ShardState{Generation: "g2", Digest: HashContent([]byte("two")), SnapshotToken: "s2", Floor: floor}
	if err := store.CommitShardState(ctx, shard, conflict); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	got, err := store.ShardState(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != first.Generation || got.Digest != first.Digest || got.SnapshotToken != first.SnapshotToken {
		t.Fatalf("conflicting shard metadata overwrote committed state: %#v", got)
	}
}

func TestFetchLeaseCompareAndDelete(t *testing.T) {
	client, store := testStore(t)
	ctx := context.Background()
	ok, err := store.AcquireFetchLease(ctx, "/x", "old", 40*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("acquire old: ok=%v err=%v", ok, err)
	}
	time.Sleep(60 * time.Millisecond)
	ok, err = store.AcquireFetchLease(ctx, "/x", "new", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire new: ok=%v err=%v", ok, err)
	}
	if err := store.ReleaseFetchLease(ctx, "/x", "old"); err != nil {
		t.Fatal(err)
	}
	keyID, _ := KeyID("test", "/x")
	got, err := client.Get(ctx, store.fetchKey(keyID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got != "new" {
		t.Fatalf("late owner deleted newer lease: %q", got)
	}
}

func TestSubscriberPersistsSoftHint(t *testing.T) {
	client, store := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	applied := make(chan struct{}, 1)
	sub, err := NewSubscriber(client, store, SubscriberConfig{HintTTL: time.Minute}, SubscriberHooks{
		OnSubscribed: func() { close(ready) },
		OnApplied: func(Hint, bool) {
			select {
			case applied <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not become ready")
	}

	obj := putObject(4, "four")
	h, err := NewHint("test", "/x", obj.State)
	if err != nil {
		t.Fatal(err)
	}
	results := (&FanoutPublisher{Namespace: "test", Islands: []Island{{Name: "local", Client: client}}}).Publish(ctx, h)
	if len(results) != 1 || results[0].Err != nil || results[0].Subscribers < 1 {
		t.Fatalf("publish result: %#v", results)
	}
	select {
	case <-applied:
	case <-time.After(2 * time.Second):
		t.Fatal("hint was not applied")
	}
	hint, err := store.Hint(ctx, "/x")
	if err != nil {
		t.Fatal(err)
	}
	if hint == nil || hint.Revision.Seq != 4 {
		t.Fatalf("unexpected hint: %#v", hint)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subscriber shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not stop")
	}
}

type fakeOrigin struct {
	mu      sync.Mutex
	objects map[string]Object
	meta    map[string]ShardMeta
	pages   map[string]SnapshotPage
}

func (f *fakeOrigin) Resolve(_ context.Context, _ string, key string) (Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	return obj, nil
}
func (f *fakeOrigin) ShardMeta(_ context.Context, _ string, shard string) (ShardMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.meta[shard]
	if !ok {
		return ShardMeta{}, errors.New("missing shard")
	}
	return m, nil
}
func (f *fakeOrigin) ShardSnapshot(_ context.Context, _, shard, token, cursor string, _ int) (SnapshotPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pages[shard+":"+token+":"+cursor]
	if !ok {
		return SnapshotPage{}, errors.New("missing snapshot page")
	}
	return p, nil
}

func TestCacheRefreshesFromSoftHintAndClearsBogusHighHint(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	if _, err := store.InstallPut(ctx, "/x", putObject(1, "one")); err != nil {
		t.Fatal(err)
	}
	bogus := AuthorityState{Revision: Revision{Era: 1, Seq: 999}, Kind: StatePut, ContentHash: HashContent([]byte("bogus"))}
	if _, err := store.AdvanceHint(ctx, "/x", bogus, time.Minute); err != nil {
		t.Fatal(err)
	}
	origin := &fakeOrigin{objects: map[string]Object{"/x": putObject(2, "two")}}
	cache, err := NewCache(store, origin, CacheConfig{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cache.Get(ctx, "/x", NQC1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Object.State.Revision.Seq != 2 || string(res.Object.Body) != "two" {
		t.Fatalf("unexpected refresh result: %#v", res)
	}
	if hint, err := store.Hint(ctx, "/x"); err != nil || hint != nil {
		t.Fatalf("bogus hint was not cleared: hint=%#v err=%v", hint, err)
	}
}

func TestReconcileInvalidatesAbsentLiveKeyAndAdvancesFloor(t *testing.T) {
	_, store := testStore(t)
	ctx := context.Background()
	if _, err := store.InstallPut(ctx, "/gone", putObject(1, "one")); err != nil {
		t.Fatal(err)
	}
	keyID, _ := KeyID("test", "/gone")
	shard, _ := ShardForKeyID(keyID, store.ShardBits())
	digest, err := DigestManifestEntries(nil)
	if err != nil {
		t.Fatal(err)
	}
	meta := ShardMeta{Shard: shard, Generation: "g2", SnapshotToken: "snap2", Digest: digest, HighWatermark: Revision{Era: 1, Seq: 10}}
	origin := &fakeOrigin{
		meta:  map[string]ShardMeta{shard: meta},
		pages: map[string]SnapshotPage{shard + ":snap2:": {Meta: meta}},
	}
	r, err := NewReconciler(store, origin, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileShard(ctx, shard); err != nil {
		t.Fatal(err)
	}
	if head, err := store.Head(ctx, "/gone"); err != nil || head != nil {
		t.Fatalf("absent key still has head: head=%#v err=%v", head, err)
	}
	state, err := store.ShardState(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if state.Floor.Seq != 10 || state.Digest != digest {
		t.Fatalf("unexpected shard state: %#v", state)
	}
}

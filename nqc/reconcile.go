package nqc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

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
	ContentType string
	ETag        string
	FreshUntil  time.Time
	StaleUntil  time.Time
}

func (e ManifestEntry) State() AuthorityState {
	return AuthorityState{
		Revision:    e.Revision,
		Kind:        StatePut,
		ContentHash: e.ContentHash,
		ContentType: e.ContentType,
		ETag:        e.ETag,
		FreshUntil:  e.FreshUntil,
		StaleUntil:  e.StaleUntil,
	}
}

type SnapshotPage struct {
	Meta       ShardMeta
	Entries    []ManifestEntry
	NextCursor string
}

type ReconcilerConfig struct {
	PageSize    int
	MaxEntries  int
	Concurrency int
	MaxKeyBytes int
}

type Reconciler struct {
	store  *RedisStore
	origin Origin
	cfg    ReconcilerConfig
}

func NewReconciler(store *RedisStore, origin Origin, cfg ReconcilerConfig) (*Reconciler, error) {
	if store == nil || origin == nil {
		return nil, errors.New("nqc: reconciler requires store and origin")
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 512
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 1_000_000
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.MaxKeyBytes <= 0 {
		cfg.MaxKeyBytes = 4096
	}
	return &Reconciler{store: store, origin: origin, cfg: cfg}, nil
}

func (r *Reconciler) ReconcileShard(ctx context.Context, shard string) error {
	remote, err := r.origin.ShardMeta(ctx, r.store.Namespace(), shard)
	if err != nil {
		return fmt.Errorf("nqc: fetch shard %s metadata: %w", shard, err)
	}
	if err := validateShardMeta(remote, shard); err != nil {
		return err
	}
	local, err := r.store.ShardState(ctx, shard)
	if err != nil {
		return err
	}
	if local.Generation == remote.Generation && local.Digest == remote.Digest && local.Floor.Compare(remote.HighWatermark) >= 0 {
		return nil
	}

	entries, err := r.readSnapshot(ctx, remote)
	if err != nil {
		return err
	}
	digest, err := DigestManifestEntries(entries)
	if err != nil {
		return err
	}
	if digest != remote.Digest {
		return fmt.Errorf("nqc: shard %s digest mismatch: want %s got %s", shard, remote.Digest, digest)
	}

	live := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := r.validateManifestEntry(entry, shard, remote.HighWatermark); err != nil {
			return err
		}
		if _, exists := live[entry.KeyID]; exists {
			return fmt.Errorf("nqc: duplicate manifest key_id %s in shard %s", entry.KeyID, shard)
		}
		live[entry.KeyID] = struct{}{}
		if _, err := r.store.ConfirmPut(ctx, entry.Key, entry.State()); err != nil {
			return fmt.Errorf("nqc: reconcile confirm %s: %w", entry.KeyID, err)
		}
	}

	after := ""
	for {
		members, next, err := r.store.ListIndex(ctx, shard, after, int64(r.cfg.PageSize))
		if err != nil {
			return err
		}
		for _, keyID := range members {
			if _, ok := live[keyID]; ok {
				continue
			}
			if err := r.store.InvalidateAbsent(ctx, keyID, remote.HighWatermark); err != nil {
				return err
			}
		}
		if next == "" {
			break
		}
		if next == after {
			return fmt.Errorf("nqc: shard %s index cursor did not advance", shard)
		}
		after = next
	}

	if err := r.store.CommitShardState(ctx, shard, ShardState{
		Generation:    remote.Generation,
		Digest:        remote.Digest,
		SnapshotToken: remote.SnapshotToken,
		Floor:         remote.HighWatermark,
	}); err != nil {
		return err
	}
	return nil
}

func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	shards := shardNames(r.store.ShardBits())
	jobs := make(chan string)
	errs := make(chan error, len(shards))
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shard := range jobs {
				if err := r.ReconcileShard(ctx, shard); err != nil {
					errs <- err
				}
			}
		}()
	}

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(errs)
			return ctx.Err()
		case jobs <- shard:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)

	var all []error
	for err := range errs {
		all = append(all, err)
	}
	return errors.Join(all...)
}

func (r *Reconciler) readSnapshot(ctx context.Context, meta ShardMeta) ([]ManifestEntry, error) {
	entries := make([]ManifestEntry, 0, r.cfg.PageSize)
	cursor := ""
	for {
		page, err := r.origin.ShardSnapshot(ctx, r.store.Namespace(), meta.Shard, meta.SnapshotToken, cursor, r.cfg.PageSize)
		if err != nil {
			return nil, fmt.Errorf("nqc: fetch shard %s snapshot page: %w", meta.Shard, err)
		}
		if !sameShardMeta(page.Meta, meta) {
			return nil, fmt.Errorf("nqc: shard %s snapshot metadata changed during pagination", meta.Shard)
		}
		if len(entries)+len(page.Entries) > r.cfg.MaxEntries {
			return nil, fmt.Errorf("nqc: shard %s snapshot exceeds max entries %d", meta.Shard, r.cfg.MaxEntries)
		}
		entries = append(entries, page.Entries...)
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			return nil, fmt.Errorf("nqc: shard %s snapshot cursor did not advance", meta.Shard)
		}
		cursor = page.NextCursor
	}
	return entries, nil
}

func (r *Reconciler) validateManifestEntry(entry ManifestEntry, shard string, floor Revision) error {
	if entry.Key == "" || len(entry.Key) > r.cfg.MaxKeyBytes {
		return fmt.Errorf("nqc: invalid manifest key length for %s", entry.KeyID)
	}
	want, err := KeyID(r.store.Namespace(), entry.Key)
	if err != nil {
		return err
	}
	if entry.KeyID != want {
		return fmt.Errorf("nqc: manifest key_id mismatch for %q", entry.Key)
	}
	gotShard, err := ShardForKeyID(entry.KeyID, r.store.ShardBits())
	if err != nil {
		return err
	}
	if gotShard != shard {
		return fmt.Errorf("nqc: manifest entry %s belongs to shard %s, not %s", entry.KeyID, gotShard, shard)
	}
	if entry.Revision.IsZero() || entry.Revision.Compare(floor) > 0 {
		return fmt.Errorf("nqc: manifest entry %s revision is outside snapshot high-watermark", entry.KeyID)
	}
	if err := validateState(entry.State(), true); err != nil {
		return err
	}
	return nil
}

func DigestManifestEntries(entries []ManifestEntry) (string, error) {
	ordered := append([]ManifestEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].KeyID < ordered[j].KeyID })
	h := sha256.New()
	var lenbuf [4]byte
	writeField := func(v string) error {
		if uint64(len(v)) > uint64(^uint32(0)) {
			return errors.New("nqc: manifest field too large")
		}
		binary.BigEndian.PutUint32(lenbuf[:], uint32(len(v)))
		_, _ = h.Write(lenbuf[:])
		_, _ = h.Write([]byte(v))
		return nil
	}
	for _, e := range ordered {
		for _, field := range []string{
			e.KeyID,
			e.Key,
			e.Revision.Wire(),
			e.ContentHash,
			e.ContentType,
			e.ETag,
			timeMillis(e.FreshUntil),
			timeMillis(e.StaleUntil),
		} {
			if err := writeField(field); err != nil {
				return "", err
			}
		}
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func validateShardMeta(meta ShardMeta, shard string) error {
	if meta.Shard != shard {
		return fmt.Errorf("nqc: shard metadata says %q, expected %q", meta.Shard, shard)
	}
	if meta.Generation == "" || meta.SnapshotToken == "" {
		return errors.New("nqc: shard metadata requires generation and snapshot token")
	}
	if meta.HighWatermark.IsZero() {
		return errors.New("nqc: shard metadata has zero high-watermark")
	}
	if err := VerifyContentDigest(meta.Digest); err != nil {
		return fmt.Errorf("nqc: invalid shard digest: %w", err)
	}
	return nil
}

func sameShardMeta(a, b ShardMeta) bool {
	return a.Shard == b.Shard &&
		a.Generation == b.Generation &&
		a.SnapshotToken == b.SnapshotToken &&
		a.Digest == b.Digest &&
		a.HighWatermark.Compare(b.HighWatermark) == 0
}

func shardNames(bits uint8) []string {
	count := 1 << bits
	width := int(bits / 4)
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = fmt.Sprintf("%0*x", width, i)
	}
	return out
}

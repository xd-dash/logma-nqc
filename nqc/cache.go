package nqc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type Policy uint8

const (
	NQC0 Policy = iota
	NQC1
	NQC2
	OriginOnly
)

type Freshness uint8

const (
	Fresh Freshness = iota + 1
	ProbablyFresh
	KnownStale
	SuspectedStale
	Shrug
	Deleted
)

type ReadResult struct {
	Object      Object
	Freshness   Freshness
	ServedStale bool
}

type Origin interface {
	Resolve(ctx context.Context, namespace, canonicalKey string) (Object, error)
	ShardMeta(ctx context.Context, namespace, shard string) (ShardMeta, error)
	ShardSnapshot(ctx context.Context, namespace, shard, token, cursor string, count int) (SnapshotPage, error)
}

type CacheConfig struct {
	LeaseTTL         time.Duration
	WaitTimeout      time.Duration
	WaitPollInterval time.Duration
	AllowStaleOnNQC2 bool
}

type Cache struct {
	store  *RedisStore
	origin Origin
	cfg    CacheConfig
}

func NewCache(store *RedisStore, origin Origin, cfg CacheConfig) (*Cache, error) {
	if store == nil || origin == nil {
		return nil, errors.New("nqc: cache requires store and origin")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 5 * time.Second
	}
	if cfg.WaitTimeout <= 0 {
		cfg.WaitTimeout = 250 * time.Millisecond
	}
	if cfg.WaitPollInterval <= 0 {
		cfg.WaitPollInterval = 25 * time.Millisecond
	}
	return &Cache{store: store, origin: origin, cfg: cfg}, nil
}

func (c *Cache) Get(ctx context.Context, canonicalKey string, policy Policy) (ReadResult, error) {
	if policy == OriginOnly {
		obj, err := c.origin.Resolve(ctx, c.store.Namespace(), canonicalKey)
		if err != nil {
			return ReadResult{}, err
		}
		if obj.State.Kind == StateDelete {
			return ReadResult{Freshness: Deleted}, ErrNotFound
		}
		if err := validateOriginObject(obj); err != nil {
			return ReadResult{}, err
		}
		return ReadResult{Object: obj, Freshness: Fresh}, nil
	}

	local, err := c.store.Head(ctx, canonicalKey)
	if err != nil {
		return ReadResult{}, err
	}
	confirmed, err := c.store.Confirmed(ctx, canonicalKey)
	if err != nil {
		return ReadResult{}, err
	}

	if confirmedDeleteWins(confirmed, local) {
		return ReadResult{Freshness: Deleted}, ErrNotFound
	}

	if policy == NQC0 {
		if local == nil || !staleUsable(local.State, time.Now()) {
			return ReadResult{Freshness: Shrug}, ErrCacheMiss
		}
		freshness := ProbablyFresh
		if confirmed != nil && confirmed.Kind == StatePut && confirmed.Revision.Compare(local.State.Revision) == 0 {
			freshness = Fresh
		}
		return ReadResult{Object: *local, Freshness: freshness}, nil
	}

	hint, err := c.store.Hint(ctx, canonicalKey)
	if err != nil {
		return ReadResult{}, err
	}
	if policy == NQC2 {
		return c.refresh(ctx, canonicalKey, local, confirmed, hint, policy)
	}

	now := time.Now()
	needRefresh, freshness := classify(local, confirmed, hint, now)
	if !needRefresh {
		return ReadResult{Object: *local, Freshness: freshness}, nil
	}
	return c.refresh(ctx, canonicalKey, local, confirmed, hint, policy)
}

func classify(local *Object, confirmed, hint *AuthorityState, now time.Time) (bool, Freshness) {
	if local == nil {
		return true, Shrug
	}
	if confirmed != nil {
		if confirmed.Kind == StateDelete && confirmed.Revision.Compare(local.State.Revision) >= 0 {
			return true, Deleted
		}
		if confirmed.Kind == StatePut {
			switch confirmed.Revision.Compare(local.State.Revision) {
			case 1:
				return true, KnownStale
			case -1:
				return true, Shrug
			}
		}
	}
	if hint != nil && hint.Revision.Compare(local.State.Revision) > 0 {
		return true, SuspectedStale
	}
	if !freshUsable(local.State, now) {
		return true, KnownStale
	}
	if confirmed != nil && confirmed.Kind == StatePut && confirmed.Revision.Compare(local.State.Revision) == 0 {
		return false, Fresh
	}
	return false, ProbablyFresh
}

func (c *Cache) refresh(
	ctx context.Context,
	canonicalKey string,
	stale *Object,
	confirmed, observedHint *AuthorityState,
	policy Policy,
) (ReadResult, error) {
	token, err := leaseToken()
	if err != nil {
		return ReadResult{}, err
	}
	acquired, err := c.store.AcquireFetchLease(ctx, canonicalKey, token, c.cfg.LeaseTTL)
	if err != nil {
		return ReadResult{}, err
	}
	if !acquired {
		return c.waitForRefresh(ctx, canonicalKey, stale, observedHint, policy)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.store.ReleaseFetchLease(releaseCtx, canonicalKey, token)
	}()

	// Re-read after winning the lease so the origin request is based on the newest local
	// information, even though soft hints are never trusted as authoritative state.
	latestConfirmed, err := c.store.Confirmed(ctx, canonicalKey)
	if err != nil {
		return c.staleOnError(stale, observedHint, policy, err)
	}
	latestHint, err := c.store.Hint(ctx, canonicalKey)
	if err != nil {
		return c.staleOnError(stale, observedHint, policy, err)
	}
	if latestConfirmed != nil {
		confirmed = latestConfirmed
	}
	if latestHint != nil {
		observedHint = latestHint
	}

	obj, err := c.origin.Resolve(ctx, c.store.Namespace(), canonicalKey)
	if err != nil {
		return c.staleOnError(stale, observedHint, policy, err)
	}
	if err := validateOriginObject(obj); err != nil {
		return c.staleOnError(stale, observedHint, policy, err)
	}
	if confirmed != nil && obj.State.Revision.Compare(confirmed.Revision) < 0 {
		return ReadResult{}, fmt.Errorf("%w: origin=%s confirmed=%s", ErrOriginRegression,
			obj.State.Revision.Wire(), confirmed.Revision.Wire())
	}

	switch obj.State.Kind {
	case StateDelete:
		advanced, err := c.store.ConfirmDelete(ctx, canonicalKey, obj.State)
		if err != nil {
			return ReadResult{}, err
		}
		if observedHint != nil && observedHint.Revision.Compare(obj.State.Revision) > 0 {
			_ = c.store.ClearHintIfEqual(ctx, canonicalKey, observedHint.Revision)
		}
		if !advanced {
			return c.resultAfterSuperseded(ctx, canonicalKey)
		}
		return ReadResult{Freshness: Deleted}, ErrNotFound
	case StatePut:
		advanced, err := c.store.InstallPut(ctx, canonicalKey, obj)
		if err != nil {
			return ReadResult{}, err
		}
		if observedHint != nil && observedHint.Revision.Compare(obj.State.Revision) > 0 {
			_ = c.store.ClearHintIfEqual(ctx, canonicalKey, observedHint.Revision)
		}
		if !advanced {
			return c.resultAfterSuperseded(ctx, canonicalKey)
		}
		return ReadResult{Object: obj, Freshness: Fresh}, nil
	default:
		return ReadResult{}, fmt.Errorf("nqc: origin returned invalid state kind %d", obj.State.Kind)
	}
}

func (c *Cache) waitForRefresh(
	ctx context.Context,
	canonicalKey string,
	stale *Object,
	observedHint *AuthorityState,
	policy Policy,
) (ReadResult, error) {
	deadline := time.NewTimer(c.cfg.WaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(c.cfg.WaitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ReadResult{}, ctx.Err()
		case <-ticker.C:
			confirmed, err := c.store.Confirmed(ctx, canonicalKey)
			if err != nil {
				return c.staleOnError(stale, observedHint, policy, err)
			}
			currentHint, err := c.store.Hint(ctx, canonicalKey)
			if err != nil {
				return c.staleOnError(stale, observedHint, policy, err)
			}
			local, err := c.store.Head(ctx, canonicalKey)
			if err != nil {
				return c.staleOnError(stale, currentHint, policy, err)
			}
			if confirmedDeleteWins(confirmed, local) {
				return ReadResult{Freshness: Deleted}, ErrNotFound
			}
			if local != nil {
				if confirmed == nil || confirmed.Kind != StatePut || confirmed.Revision.Compare(local.State.Revision) <= 0 {
					if currentHint == nil || currentHint.Revision.Compare(local.State.Revision) <= 0 {
						return ReadResult{Object: *local, Freshness: Fresh}, nil
					}
				}
			}
		case <-deadline.C:
			return c.staleOnError(stale, observedHint, policy, ErrRefreshInProgress)
		}
	}
}

func (c *Cache) staleOnError(stale *Object, hint *AuthorityState, policy Policy, cause error) (ReadResult, error) {
	if errors.Is(cause, ErrNotFound) {
		return ReadResult{Freshness: Deleted}, cause
	}
	if stale == nil || !staleUsable(stale.State, time.Now()) {
		return ReadResult{Freshness: Shrug}, cause
	}
	if hint != nil && hint.Kind == StateDelete && hint.Revision.Compare(stale.State.Revision) > 0 {
		return ReadResult{Freshness: SuspectedStale}, cause
	}
	if policy == NQC2 && !c.cfg.AllowStaleOnNQC2 {
		return ReadResult{Freshness: Shrug}, cause
	}
	return ReadResult{Object: *stale, Freshness: KnownStale, ServedStale: true}, nil
}

func (c *Cache) resultAfterSuperseded(ctx context.Context, canonicalKey string) (ReadResult, error) {
	confirmed, err := c.store.Confirmed(ctx, canonicalKey)
	if err != nil {
		return ReadResult{}, err
	}
	local, err := c.store.Head(ctx, canonicalKey)
	if err != nil {
		return ReadResult{}, err
	}
	if confirmedDeleteWins(confirmed, local) {
		return ReadResult{Freshness: Deleted}, ErrNotFound
	}
	if local != nil {
		if confirmed == nil || confirmed.Kind != StatePut || confirmed.Revision.Compare(local.State.Revision) <= 0 {
			return ReadResult{Object: *local, Freshness: Fresh}, nil
		}
	}
	return ReadResult{Freshness: Shrug}, ErrRefreshInProgress
}

func confirmedDeleteWins(confirmed *AuthorityState, local *Object) bool {
	if confirmed == nil || confirmed.Kind != StateDelete {
		return false
	}
	return local == nil || confirmed.Revision.Compare(local.State.Revision) >= 0
}

func freshUsable(state AuthorityState, now time.Time) bool {
	return state.FreshUntil.IsZero() || now.Before(state.FreshUntil) || now.Equal(state.FreshUntil)
}

func staleUsable(state AuthorityState, now time.Time) bool {
	return state.StaleUntil.IsZero() || now.Before(state.StaleUntil) || now.Equal(state.StaleUntil)
}

func validateOriginObject(obj Object) error {
	if err := validateState(obj.State, obj.State.Kind == StatePut); err != nil {
		return err
	}
	if obj.State.Kind == StatePut {
		return VerifyContent(obj.State.ContentHash, obj.Body)
	}
	if len(obj.Body) != 0 {
		return errors.New("nqc: delete response must not contain body")
	}
	return nil
}

func leaseToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("nqc: generate lease token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

package nqc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type StoreConfig struct {
	Namespace      string
	ShardBits      uint8
	DefaultDataTTL time.Duration
	DataTTLGrace   time.Duration
}

type RedisStore struct {
	client         *redis.Client
	namespace      string
	shardBits      uint8
	defaultDataTTL time.Duration
	dataTTLGrace   time.Duration
}

func NewRedisStore(client *redis.Client, cfg StoreConfig) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("nqc: nil redis client")
	}
	if err := ValidateNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	if cfg.ShardBits == 0 {
		cfg.ShardBits = 8
	}
	if cfg.ShardBits < 4 || cfg.ShardBits > 16 || cfg.ShardBits%4 != 0 {
		return nil, errors.New("nqc: shard bits must be 4, 8, 12, or 16")
	}
	if cfg.DefaultDataTTL <= 0 {
		cfg.DefaultDataTTL = 24 * time.Hour
	}
	if cfg.DataTTLGrace <= 0 {
		cfg.DataTTLGrace = time.Minute
	}
	return &RedisStore{
		client:         client,
		namespace:      cfg.Namespace,
		shardBits:      cfg.ShardBits,
		defaultDataTTL: cfg.DefaultDataTTL,
		dataTTLGrace:   cfg.DataTTLGrace,
	}, nil
}

func (s *RedisStore) Client() *redis.Client { return s.client }
func (s *RedisStore) Namespace() string     { return s.namespace }
func (s *RedisStore) ShardBits() uint8      { return s.shardBits }

func (s *RedisStore) keyID(canonicalKey string) (string, error) {
	return KeyID(s.namespace, canonicalKey)
}

func (s *RedisStore) shard(keyID string) (string, error) {
	return ShardForKeyID(keyID, s.shardBits)
}

func (s *RedisStore) validateShardID(shard string) error {
	if len(shard) != int(s.shardBits/4) {
		return errors.New("nqc: invalid shard id length")
	}
	for _, r := range shard {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return errors.New("nqc: shard id must be lowercase hex")
		}
	}
	return nil
}

func (s *RedisStore) dataKey(keyID string, rev Revision) string {
	return "nqc:data:" + s.namespace + ":" + keyID + ":" + rev.Wire()
}
func (s *RedisStore) headKey(keyID string) string {
	return "nqc:head:" + s.namespace + ":" + keyID
}
func (s *RedisStore) confirmedKey(keyID string) string {
	return "nqc:confirmed:" + s.namespace + ":" + keyID
}
func (s *RedisStore) hintKey(keyID string) string {
	return "nqc:hint:" + s.namespace + ":" + keyID
}
func (s *RedisStore) floorKey(shard string) string {
	return "nqc:floor:" + s.namespace + ":" + shard
}
func (s *RedisStore) manifestKey(shard string) string {
	return "nqc:manifest:" + s.namespace + ":" + shard
}
func (s *RedisStore) indexKey(shard string) string {
	return "nqc:index:" + s.namespace + ":" + shard
}
func (s *RedisStore) fetchKey(keyID string) string {
	return "nqc:fetch:" + s.namespace + ":" + keyID
}

var advanceHintScript = redis.NewScript(`
local incoming = ARGV[1]
local confirmed = redis.call('HGET', KEYS[2], 'rev')
local confirmed_kind = redis.call('HGET', KEYS[2], 'kind')
if confirmed and confirmed >= incoming then
  return 0
end
local floor = redis.call('GET', KEYS[3])
if (not confirmed or confirmed_kind ~= 'put') and floor and floor >= incoming then
  return -1
end
local current = redis.call('HGET', KEYS[1], 'rev')
if current and current >= incoming then
  return 0
end
redis.call('HSET', KEYS[1],
  'rev', ARGV[1],
  'kind', ARGV[2],
  'content_hash', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1
`)

var confirmPutScript = redis.NewScript(`
local incoming = ARGV[1]
local current = redis.call('HGET', KEYS[1], 'rev')
if current and current > incoming then
  return 0
end
if current and current == incoming then
  if redis.call('HGET', KEYS[1], 'kind') ~= 'put'
    or redis.call('HGET', KEYS[1], 'content_hash') ~= ARGV[2]
    or redis.call('HGET', KEYS[1], 'content_type') ~= ARGV[3]
    or redis.call('HGET', KEYS[1], 'etag') ~= ARGV[4]
    or redis.call('HGET', KEYS[1], 'fresh_ms') ~= ARGV[5]
    or redis.call('HGET', KEYS[1], 'stale_ms') ~= ARGV[6] then
    return -2
  end
else
  redis.call('HSET', KEYS[1],
    'rev', ARGV[1],
    'kind', 'put',
    'content_hash', ARGV[2],
    'content_type', ARGV[3],
    'etag', ARGV[4],
    'fresh_ms', ARGV[5],
    'stale_ms', ARGV[6])
end
redis.call('ZADD', KEYS[2], 0, ARGV[7])
local hint = redis.call('HGET', KEYS[3], 'rev')
if hint and hint <= incoming then
  redis.call('DEL', KEYS[3])
end
return 1
`)

var installPutScript = redis.NewScript(`
local incoming = ARGV[1]
local current = redis.call('HGET', KEYS[1], 'rev')
if current and current > incoming then
  return 0
end
if current and current == incoming then
  if redis.call('HGET', KEYS[1], 'kind') ~= 'put'
    or redis.call('HGET', KEYS[1], 'content_hash') ~= ARGV[2]
    or redis.call('HGET', KEYS[1], 'content_type') ~= ARGV[3]
    or redis.call('HGET', KEYS[1], 'etag') ~= ARGV[4]
    or redis.call('HGET', KEYS[1], 'fresh_ms') ~= ARGV[5]
    or redis.call('HGET', KEYS[1], 'stale_ms') ~= ARGV[6] then
    return -2
  end
else
  redis.call('HSET', KEYS[1],
    'rev', ARGV[1],
    'kind', 'put',
    'content_hash', ARGV[2],
    'content_type', ARGV[3],
    'etag', ARGV[4],
    'fresh_ms', ARGV[5],
    'stale_ms', ARGV[6])
end
local head = redis.call('GET', KEYS[2])
if not head or head < incoming then
  redis.call('SET', KEYS[2], incoming)
end
redis.call('ZADD', KEYS[3], 0, ARGV[7])
local hint = redis.call('HGET', KEYS[4], 'rev')
if hint and hint <= incoming then
  redis.call('DEL', KEYS[4])
end
return 1
`)

var confirmDeleteScript = redis.NewScript(`
local incoming = ARGV[1]
local current = redis.call('HGET', KEYS[1], 'rev')
if current and current > incoming then
  return 0
end
if current and current == incoming then
  if redis.call('HGET', KEYS[1], 'kind') ~= 'delete' then
    return -2
  end
else
  redis.call('HSET', KEYS[1], 'rev', incoming, 'kind', 'delete',
    'content_hash', '', 'content_type', '', 'etag', '', 'fresh_ms', '0', 'stale_ms', '0')
end
redis.call('ZREM', KEYS[3], ARGV[2])
local head = redis.call('GET', KEYS[2])
if head and head <= incoming then
  redis.call('DEL', KEYS[2])
end
local hint = redis.call('HGET', KEYS[4], 'rev')
if hint and hint <= incoming then
  redis.call('DEL', KEYS[4])
end
return 1
`)

var invalidateAbsentScript = redis.NewScript(`
local floor = ARGV[1]
local future_live = false
local confirmed = redis.call('HGET', KEYS[1], 'rev')
local confirmed_kind = redis.call('HGET', KEYS[1], 'kind')
if confirmed then
  if confirmed <= floor then
    redis.call('DEL', KEYS[1])
  elseif confirmed_kind == 'put' then
    future_live = true
  end
end
local head = redis.call('GET', KEYS[2])
if head then
  if head <= floor then
    redis.call('DEL', KEYS[2])
  else
    future_live = true
  end
end
local hint = redis.call('HGET', KEYS[4], 'rev')
if hint and hint <= floor then
  redis.call('DEL', KEYS[4])
end
if not future_live then
  redis.call('ZREM', KEYS[3], ARGV[2])
end
return 1
`)

var advanceFloorScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current and current > ARGV[1] then
  return 0
end
if current and current == ARGV[1] then
  local generation = redis.call('HGET', KEYS[2], 'generation')
  local digest = redis.call('HGET', KEYS[2], 'digest')
  local snapshot_token = redis.call('HGET', KEYS[2], 'snapshot_token')
  if generation == ARGV[2] and digest == ARGV[3] and snapshot_token == ARGV[4] then
    return 0
  end
  return -2
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2],
  'generation', ARGV[2],
  'digest', ARGV[3],
  'snapshot_token', ARGV[4],
  'floor', ARGV[1])
return 1
`)

var releaseLeaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

var clearHintIfEqualScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'rev') == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

var clearHeadIfEqualScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

func (s *RedisStore) Head(ctx context.Context, canonicalKey string) (*Object, error) {
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return nil, err
	}
	wire, err := s.client.Get(ctx, s.headKey(keyID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("nqc: read head: %w", err)
	}
	rev, err := ParseRevision(wire)
	if err != nil {
		return nil, err
	}
	obj, err := s.objectByRevision(ctx, keyID, rev)
	if errors.Is(err, redis.Nil) {
		_, _ = clearHeadIfEqualScript.Run(ctx, s.client, []string{s.headKey(keyID)}, wire).Result()
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *RedisStore) Confirmed(ctx context.Context, canonicalKey string) (*AuthorityState, error) {
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return nil, err
	}
	return s.readState(ctx, s.confirmedKey(keyID))
}

func (s *RedisStore) Hint(ctx context.Context, canonicalKey string) (*AuthorityState, error) {
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return nil, err
	}
	return s.readState(ctx, s.hintKey(keyID))
}

func (s *RedisStore) AdvanceHint(ctx context.Context, canonicalKey string, state AuthorityState, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, errors.New("nqc: hint ttl must be positive")
	}
	if err := validateState(state, state.Kind == StatePut); err != nil {
		return false, err
	}
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return false, err
	}
	shard, err := s.shard(keyID)
	if err != nil {
		return false, err
	}
	res, err := advanceHintScript.Run(ctx, s.client,
		[]string{s.hintKey(keyID), s.confirmedKey(keyID), s.floorKey(shard)},
		state.Revision.Wire(), state.Kind.String(), state.ContentHash, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("nqc: advance hint: %w", err)
	}
	return res == 1, nil
}

func (s *RedisStore) ConfirmPut(ctx context.Context, canonicalKey string, state AuthorityState) (bool, error) {
	if state.Kind != StatePut {
		return false, errors.New("nqc: ConfirmPut requires put state")
	}
	if err := validateState(state, true); err != nil {
		return false, err
	}
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return false, err
	}
	shard, err := s.shard(keyID)
	if err != nil {
		return false, err
	}
	res, err := confirmPutScript.Run(ctx, s.client,
		[]string{s.confirmedKey(keyID), s.indexKey(shard), s.hintKey(keyID)},
		stateArgs(state, keyID)...).Int64()
	if err != nil {
		return false, fmt.Errorf("nqc: confirm put: %w", err)
	}
	if res == -2 {
		return false, ErrRevisionConflict
	}
	return res == 1, nil
}

func (s *RedisStore) InstallPut(ctx context.Context, canonicalKey string, obj Object) (bool, error) {
	if obj.State.Kind != StatePut {
		return false, errors.New("nqc: InstallPut requires put state")
	}
	if err := validateState(obj.State, true); err != nil {
		return false, err
	}
	if err := VerifyContent(obj.State.ContentHash, obj.Body); err != nil {
		return false, err
	}
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return false, err
	}
	shard, err := s.shard(keyID)
	if err != nil {
		return false, err
	}
	dataKey := s.dataKey(keyID, obj.State.Revision)
	ttl := s.dataTTL(obj.State)
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, dataKey, objectFields(obj))
		pipe.Expire(ctx, dataKey, ttl)
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("nqc: write immutable object: %w", err)
	}
	res, err := installPutScript.Run(ctx, s.client,
		[]string{s.confirmedKey(keyID), s.headKey(keyID), s.indexKey(shard), s.hintKey(keyID)},
		stateArgs(obj.State, keyID)...).Int64()
	if err != nil {
		return false, fmt.Errorf("nqc: install put: %w", err)
	}
	if res == -2 {
		return false, ErrRevisionConflict
	}
	return res == 1, nil
}

func (s *RedisStore) ConfirmDelete(ctx context.Context, canonicalKey string, state AuthorityState) (bool, error) {
	if state.Kind != StateDelete {
		return false, errors.New("nqc: ConfirmDelete requires delete state")
	}
	if err := validateState(state, false); err != nil {
		return false, err
	}
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return false, err
	}
	shard, err := s.shard(keyID)
	if err != nil {
		return false, err
	}
	res, err := confirmDeleteScript.Run(ctx, s.client,
		[]string{s.confirmedKey(keyID), s.headKey(keyID), s.indexKey(shard), s.hintKey(keyID)},
		state.Revision.Wire(), keyID).Int64()
	if err != nil {
		return false, fmt.Errorf("nqc: confirm delete: %w", err)
	}
	if res == -2 {
		return false, ErrRevisionConflict
	}
	return res == 1, nil
}

func (s *RedisStore) ClearHintIfEqual(ctx context.Context, canonicalKey string, rev Revision) error {
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return err
	}
	if _, err := clearHintIfEqualScript.Run(ctx, s.client, []string{s.hintKey(keyID)}, rev.Wire()).Result(); err != nil {
		return fmt.Errorf("nqc: clear hint: %w", err)
	}
	return nil
}

func (s *RedisStore) InvalidateAbsent(ctx context.Context, keyID string, floor Revision) error {
	if err := ValidateKeyID(keyID); err != nil {
		return err
	}
	shard, err := s.shard(keyID)
	if err != nil {
		return err
	}
	if _, err := invalidateAbsentScript.Run(ctx, s.client,
		[]string{s.confirmedKey(keyID), s.headKey(keyID), s.indexKey(shard), s.hintKey(keyID)},
		floor.Wire(), keyID).Result(); err != nil {
		return fmt.Errorf("nqc: invalidate absent key: %w", err)
	}
	return nil
}

func (s *RedisStore) AcquireFetchLease(ctx context.Context, canonicalKey, token string, ttl time.Duration) (bool, error) {
	if token == "" || ttl <= 0 {
		return false, errors.New("nqc: lease token and positive ttl required")
	}
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return false, err
	}
	ok, err := s.client.SetNX(ctx, s.fetchKey(keyID), token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("nqc: acquire fetch lease: %w", err)
	}
	return ok, nil
}

func (s *RedisStore) ReleaseFetchLease(ctx context.Context, canonicalKey, token string) error {
	keyID, err := s.keyID(canonicalKey)
	if err != nil {
		return err
	}
	if _, err := releaseLeaseScript.Run(ctx, s.client, []string{s.fetchKey(keyID)}, token).Result(); err != nil {
		return fmt.Errorf("nqc: release fetch lease: %w", err)
	}
	return nil
}

type ShardState struct {
	Generation    string
	Digest        string
	SnapshotToken string
	Floor         Revision
}

func (s *RedisStore) ShardState(ctx context.Context, shard string) (ShardState, error) {
	if err := s.validateShardID(shard); err != nil {
		return ShardState{}, err
	}
	fields, err := s.client.HGetAll(ctx, s.manifestKey(shard)).Result()
	if err != nil {
		return ShardState{}, fmt.Errorf("nqc: read shard state: %w", err)
	}
	if len(fields) == 0 {
		return ShardState{}, nil
	}
	floor, err := ParseRevision(fields["floor"])
	if err != nil {
		return ShardState{}, fmt.Errorf("nqc: parse shard floor: %w", err)
	}
	return ShardState{
		Generation:    fields["generation"],
		Digest:        fields["digest"],
		SnapshotToken: fields["snapshot_token"],
		Floor:         floor,
	}, nil
}

func (s *RedisStore) CommitShardState(ctx context.Context, shard string, state ShardState) error {
	if err := s.validateShardID(shard); err != nil {
		return err
	}
	if state.Floor.IsZero() {
		return errors.New("nqc: zero shard floor")
	}
	res, err := advanceFloorScript.Run(ctx, s.client,
		[]string{s.floorKey(shard), s.manifestKey(shard)},
		state.Floor.Wire(), state.Generation, state.Digest, state.SnapshotToken).Int64()
	if err != nil {
		return fmt.Errorf("nqc: commit shard state: %w", err)
	}
	if res == -2 {
		return fmt.Errorf("%w: conflicting shard metadata at floor %s", ErrRevisionConflict, state.Floor.Wire())
	}
	return nil
}

func (s *RedisStore) ListIndex(ctx context.Context, shard, after string, count int64) ([]string, string, error) {
	if err := s.validateShardID(shard); err != nil {
		return nil, "", err
	}
	if count <= 0 {
		count = 256
	}
	min := "-"
	if after != "" {
		min = "(" + after
	}
	members, err := s.client.ZRangeByLex(ctx, s.indexKey(shard), &redis.ZRangeBy{
		Min: min, Max: "+", Offset: 0, Count: count,
	}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("nqc: list shard index: %w", err)
	}
	next := ""
	if int64(len(members)) == count {
		next = members[len(members)-1]
	}
	return members, next, nil
}

func (s *RedisStore) readState(ctx context.Context, key string) (*AuthorityState, error) {
	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("nqc: read state: %w", err)
	}
	if len(fields) == 0 {
		return nil, nil
	}
	state, err := stateFromFields(fields)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *RedisStore) objectByRevision(ctx context.Context, keyID string, rev Revision) (*Object, error) {
	fields, err := s.client.HGetAll(ctx, s.dataKey(keyID, rev)).Result()
	if err != nil {
		return nil, fmt.Errorf("nqc: read cached object: %w", err)
	}
	if len(fields) == 0 {
		return nil, redis.Nil
	}
	state, err := stateFromFields(fields)
	if err != nil {
		return nil, err
	}
	if state.Kind != StatePut || state.Revision.Compare(rev) != 0 {
		return nil, fmt.Errorf("%w: cached object metadata mismatch", ErrCorruptObject)
	}
	body := []byte(fields["body"])
	if err := VerifyContent(state.ContentHash, body); err != nil {
		return nil, err
	}
	return &Object{State: state, Body: body}, nil
}

func (s *RedisStore) dataTTL(state AuthorityState) time.Duration {
	ttl := s.defaultDataTTL
	if !state.StaleUntil.IsZero() {
		until := time.Until(state.StaleUntil) + s.dataTTLGrace
		if until > ttl {
			ttl = until
		}
	}
	if ttl < time.Second {
		ttl = time.Second
	}
	return ttl
}

func stateArgs(state AuthorityState, keyID string) []any {
	return []any{
		state.Revision.Wire(), state.ContentHash, state.ContentType, state.ETag,
		timeMillis(state.FreshUntil), timeMillis(state.StaleUntil), keyID,
	}
}

func objectFields(obj Object) map[string]any {
	return map[string]any{
		"rev":          obj.State.Revision.Wire(),
		"kind":         obj.State.Kind.String(),
		"content_hash": obj.State.ContentHash,
		"content_type": obj.State.ContentType,
		"etag":         obj.State.ETag,
		"fresh_ms":     timeMillis(obj.State.FreshUntil),
		"stale_ms":     timeMillis(obj.State.StaleUntil),
		"body":         obj.Body,
	}
}

func stateFromFields(fields map[string]string) (AuthorityState, error) {
	rev, err := ParseRevision(fields["rev"])
	if err != nil {
		return AuthorityState{}, err
	}
	kind, err := parseStateKind(fields["kind"])
	if err != nil {
		return AuthorityState{}, err
	}
	fresh, err := parseMillis(fields["fresh_ms"])
	if err != nil {
		return AuthorityState{}, fmt.Errorf("nqc: parse fresh_until: %w", err)
	}
	stale, err := parseMillis(fields["stale_ms"])
	if err != nil {
		return AuthorityState{}, fmt.Errorf("nqc: parse stale_until: %w", err)
	}
	state := AuthorityState{
		Revision:    rev,
		Kind:        kind,
		ContentHash: fields["content_hash"],
		ContentType: fields["content_type"],
		ETag:        fields["etag"],
		FreshUntil:  fresh,
		StaleUntil:  stale,
	}
	if err := validateState(state, kind == StatePut); err != nil {
		return AuthorityState{}, err
	}
	return state, nil
}

func timeMillis(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UTC().UnixMilli(), 10)
}

func parseMillis(s string) (time.Time, error) {
	if s == "" || s == "0" {
		return time.Time{}, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(v).UTC(), nil
}

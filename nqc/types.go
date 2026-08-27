package nqc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var (
	ErrCacheMiss         = errors.New("nqc: cache miss")
	ErrNotFound          = errors.New("nqc: not found")
	ErrRefreshInProgress = errors.New("nqc: refresh in progress")
	ErrOriginRegression  = errors.New("nqc: origin revision regressed")
	ErrRevisionConflict  = errors.New("nqc: conflicting state for revision")
	ErrCorruptObject     = errors.New("nqc: corrupt cached object")
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

func (r Revision) IsZero() bool {
	return r.Era == 0 && r.Seq == 0
}

func (r Revision) Wire() string {
	return fmt.Sprintf("%016x%016x", r.Era, r.Seq)
}

func ParseRevision(s string) (Revision, error) {
	if len(s) != 32 || strings.ToLower(s) != s {
		return Revision{}, fmt.Errorf("nqc: invalid revision %q", s)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return Revision{}, fmt.Errorf("nqc: invalid revision %q: %w", s, err)
	}
	era, err := strconv.ParseUint(s[:16], 16, 64)
	if err != nil {
		return Revision{}, fmt.Errorf("nqc: parse revision era: %w", err)
	}
	seq, err := strconv.ParseUint(s[16:], 16, 64)
	if err != nil {
		return Revision{}, fmt.Errorf("nqc: parse revision sequence: %w", err)
	}
	return Revision{Era: era, Seq: seq}, nil
}

type StateKind uint8

const (
	StatePut StateKind = iota + 1
	StateDelete
)

func (k StateKind) String() string {
	switch k {
	case StatePut:
		return "put"
	case StateDelete:
		return "delete"
	default:
		return "unknown"
	}
}

func parseStateKind(s string) (StateKind, error) {
	switch s {
	case "put":
		return StatePut, nil
	case "delete":
		return StateDelete, nil
	default:
		return 0, fmt.Errorf("nqc: invalid state kind %q", s)
	}
}

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

type Hint struct {
	Version     int    `json:"v"`
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	KeyID       string `json:"key_id"`
	Revision    string `json:"revision"`
	Op          string `json:"op"`
	ContentHash string `json:"content_hash,omitempty"`
}

func NewHint(namespace, key string, state AuthorityState) (Hint, error) {
	keyID, err := KeyID(namespace, key)
	if err != nil {
		return Hint{}, err
	}
	if err := validateState(state, state.Kind == StatePut); err != nil {
		return Hint{}, err
	}
	return Hint{
		Version:     1,
		Namespace:   namespace,
		Key:         key,
		KeyID:       keyID,
		Revision:    state.Revision.Wire(),
		Op:          state.Kind.String(),
		ContentHash: state.ContentHash,
	}, nil
}

func (h Hint) Validate(expectedNamespace string, maxKeyBytes int) (AuthorityState, error) {
	if h.Version != 1 {
		return AuthorityState{}, fmt.Errorf("nqc: unsupported hint version %d", h.Version)
	}
	if err := ValidateNamespace(h.Namespace); err != nil {
		return AuthorityState{}, err
	}
	if expectedNamespace != "" && h.Namespace != expectedNamespace {
		return AuthorityState{}, fmt.Errorf("nqc: hint namespace %q does not match %q", h.Namespace, expectedNamespace)
	}
	if h.Key == "" {
		return AuthorityState{}, errors.New("nqc: empty hint key")
	}
	if maxKeyBytes > 0 && len(h.Key) > maxKeyBytes {
		return AuthorityState{}, fmt.Errorf("nqc: hint key exceeds %d bytes", maxKeyBytes)
	}
	wantKeyID, err := KeyID(h.Namespace, h.Key)
	if err != nil {
		return AuthorityState{}, err
	}
	if h.KeyID != wantKeyID {
		return AuthorityState{}, errors.New("nqc: hint key_id does not match canonical key")
	}
	rev, err := ParseRevision(h.Revision)
	if err != nil {
		return AuthorityState{}, err
	}
	kind, err := parseStateKind(h.Op)
	if err != nil {
		return AuthorityState{}, err
	}
	state := AuthorityState{Revision: rev, Kind: kind, ContentHash: h.ContentHash}
	if err := validateState(state, kind == StatePut); err != nil {
		return AuthorityState{}, err
	}
	return state, nil
}

func (h Hint) Marshal() ([]byte, error) {
	return json.Marshal(h)
}

func DecodeHint(payload string, maxBytes int) (Hint, error) {
	if maxBytes > 0 && len(payload) > maxBytes {
		return Hint{}, fmt.Errorf("nqc: hint exceeds %d bytes", maxBytes)
	}
	var h Hint
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		return Hint{}, fmt.Errorf("nqc: decode hint: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Hint{}, errors.New("nqc: hint contains trailing JSON value")
		}
		return Hint{}, fmt.Errorf("nqc: decode trailing hint data: %w", err)
	}
	return h, nil
}

func VerifyContent(hash string, body []byte) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(hash, prefix) || len(hash) != len(prefix)+64 {
		return fmt.Errorf("%w: invalid sha256 digest", ErrCorruptObject)
	}
	want := hash[len(prefix):]
	if strings.ToLower(want) != want {
		return fmt.Errorf("%w: non-canonical sha256 digest", ErrCorruptObject)
	}
	if _, err := hex.DecodeString(want); err != nil {
		return fmt.Errorf("%w: invalid sha256 digest: %v", ErrCorruptObject, err)
	}
	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])
	if got != want {
		return fmt.Errorf("%w: want %s got %s", ErrCorruptObject, want, got)
	}
	return nil
}

func HashContent(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validateState(s AuthorityState, requireHash bool) error {
	if s.Revision.IsZero() {
		return errors.New("nqc: zero revision is reserved")
	}
	if s.Kind != StatePut && s.Kind != StateDelete {
		return fmt.Errorf("nqc: invalid state kind %d", s.Kind)
	}
	if requireHash {
		if err := VerifyContentDigest(s.ContentHash); err != nil {
			return err
		}
	}
	if s.Kind == StateDelete && s.ContentHash != "" {
		return errors.New("nqc: delete state must not carry content hash")
	}
	if !s.FreshUntil.IsZero() && !s.StaleUntil.IsZero() && s.StaleUntil.Before(s.FreshUntil) {
		return errors.New("nqc: stale_until precedes fresh_until")
	}
	return nil
}

func VerifyContentDigest(hash string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(hash, prefix) || len(hash) != len(prefix)+64 {
		return errors.New("nqc: content hash must be canonical sha256:<64 lowercase hex>")
	}
	d := hash[len(prefix):]
	if d != strings.ToLower(d) {
		return errors.New("nqc: content hash must use lowercase hex")
	}
	if _, err := hex.DecodeString(d); err != nil {
		return fmt.Errorf("nqc: invalid content hash: %w", err)
	}
	return nil
}

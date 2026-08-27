package nqc

import (
	"strings"
	"testing"
	"time"
)

func TestRevisionWireSortsLexicographically(t *testing.T) {
	revs := []Revision{
		{Era: 1, Seq: 1},
		{Era: 1, Seq: 2},
		{Era: 2, Seq: 0},
		{Era: 2, Seq: 1},
	}
	for i := 0; i < len(revs)-1; i++ {
		if revs[i].Compare(revs[i+1]) >= 0 {
			t.Fatalf("revision order broken: %v >= %v", revs[i], revs[i+1])
		}
		if !(revs[i].Wire() < revs[i+1].Wire()) {
			t.Fatalf("wire order broken: %s >= %s", revs[i].Wire(), revs[i+1].Wire())
		}
		got, err := ParseRevision(revs[i].Wire())
		if err != nil || got != revs[i] {
			t.Fatalf("round trip %v: got=%v err=%v", revs[i], got, err)
		}
	}
}

func TestHintValidation(t *testing.T) {
	body := []byte("hello")
	state := AuthorityState{Revision: Revision{Era: 1, Seq: 7}, Kind: StatePut, ContentHash: HashContent(body)}
	h, err := NewHint("public-assets", "/a.txt", state)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Validate("public-assets", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != state.Revision || got.Kind != StatePut || got.ContentHash != state.ContentHash {
		t.Fatalf("unexpected state: %#v", got)
	}

	h.KeyID = strings.Repeat("0", 64)
	if _, err := h.Validate("public-assets", 4096); err == nil {
		t.Fatal("expected key_id validation error")
	}
}

func TestDecodeHintRejectsTrailingJSON(t *testing.T) {
	if _, err := DecodeHint(`{"v":1} {"v":1}`, 1024); err == nil {
		t.Fatal("expected trailing JSON error")
	}
}

func TestManifestDigestDeterministic(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := ManifestEntry{
		Key: "/a", KeyID: strings.Repeat("a", 64), Revision: Revision{Era: 1, Seq: 1},
		ContentHash: HashContent([]byte("a")), ContentType: "text/plain", FreshUntil: now,
	}
	b := ManifestEntry{
		Key: "/b", KeyID: strings.Repeat("b", 64), Revision: Revision{Era: 1, Seq: 2},
		ContentHash: HashContent([]byte("b")), ContentType: "text/plain", FreshUntil: now,
	}
	d1, err := DigestManifestEntries([]ManifestEntry{a, b})
	if err != nil {
		t.Fatal(err)
	}
	d2, err := DigestManifestEntries([]ManifestEntry{b, a})
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatalf("digest depends on input order: %s != %s", d1, d2)
	}
}

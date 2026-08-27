package nqc

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

var namespaceRE = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)

func ValidateNamespace(namespace string) error {
	if !namespaceRE.MatchString(namespace) {
		return errors.New("nqc: namespace must match [a-z0-9._-]{1,64}")
	}
	return nil
}

func KeyID(namespace, canonicalKey string) (string, error) {
	if err := ValidateNamespace(namespace); err != nil {
		return "", err
	}
	if canonicalKey == "" {
		return "", errors.New("nqc: canonical cache key is empty")
	}
	h := sha256.New()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(canonicalKey))
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ValidateKeyID(keyID string) error {
	if len(keyID) != 64 {
		return errors.New("nqc: key_id must be 64 lowercase hex characters")
	}
	if _, err := hex.DecodeString(keyID); err != nil {
		return fmt.Errorf("nqc: invalid key_id: %w", err)
	}
	for _, r := range keyID {
		if r >= 'A' && r <= 'F' {
			return errors.New("nqc: key_id must use lowercase hex")
		}
	}
	return nil
}

func ShardForKeyID(keyID string, shardBits uint8) (string, error) {
	if err := ValidateKeyID(keyID); err != nil {
		return "", err
	}
	if shardBits < 4 || shardBits > 16 || shardBits%4 != 0 {
		return "", errors.New("nqc: shard bits must be 4, 8, 12, or 16")
	}
	return keyID[:int(shardBits/4)], nil
}

func UpdateChannel(namespace string) (string, error) {
	if err := ValidateNamespace(namespace); err != nil {
		return "", err
	}
	return "nqc:v1:" + namespace + ":update", nil
}

package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

var ErrPayloadNotFound = errors.New("connector input payload not found")

// PayloadResolver resolves one bounded typed connector payload by its
// operation input hash. Implementations may read from the artifact/evidence
// authority; they must not return credentials or log the payload.
type PayloadResolver func(context.Context, string, string) ([]byte, error)

// StaticPayloadResolver is a deterministic offline resolver for unit tests,
// local development, and replay fixtures. It defensively copies all bytes.
type StaticPayloadResolver struct {
	mu     sync.RWMutex
	values map[string][]byte
}

func NewStaticPayloadResolver() *StaticPayloadResolver {
	return &StaticPayloadResolver{values: make(map[string][]byte)}
}

func (r *StaticPayloadResolver) Put(workspaceID, inputHash string, payload []byte) error {
	if r == nil {
		return fmt.Errorf("payload resolver is nil")
	}
	if workspaceID == "" || !isSHA256(inputHash) || len(payload) == 0 || !hashMatches(payload, inputHash) {
		return fmt.Errorf("payload identity is invalid")
	}
	key := workspaceID + "\x00" + inputHash
	r.mu.Lock()
	if _, exists := r.values[key]; exists {
		r.mu.Unlock()
		return fmt.Errorf("payload already exists")
	}
	r.values[key] = append([]byte(nil), payload...)
	r.mu.Unlock()
	return nil
}

func (r *StaticPayloadResolver) Resolve(_ context.Context, workspaceID, inputHash string) ([]byte, error) {
	if r == nil {
		return nil, ErrPayloadNotFound
	}
	r.mu.RLock()
	payload, ok := r.values[workspaceID+"\x00"+inputHash]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrPayloadNotFound
	}
	return append([]byte(nil), payload...), nil
}

func HashPayload(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func VerifyPayload(payload []byte, inputHash string) bool {
	return isSHA256(inputHash) && hashMatches(payload, inputHash)
}

func hashMatches(payload []byte, inputHash string) bool {
	return HashPayload(payload) == inputHash
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

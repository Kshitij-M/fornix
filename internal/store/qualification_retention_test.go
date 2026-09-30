package store

import (
	"context"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestQualificationRetentionCursorIsBoundedAndTyped(t *testing.T) {
	kind, id, err := parseQualificationRetentionCursor("readiness_snapshot:snapshot-one")
	if err != nil || kind != contracts.QualificationRecordSnapshot || id != "snapshot-one" {
		t.Fatalf("cursor=%q/%q err=%v", kind, id, err)
	}
	if _, _, err := parseQualificationRetentionCursor("unknown:snapshot-one"); err == nil {
		t.Fatal("unknown retention cursor kind was accepted")
	}
	if _, _, err := parseQualificationRetentionCursor("readiness_snapshot:"); err == nil {
		t.Fatal("empty retention cursor identity was accepted")
	}
}

func TestQualificationRetentionStoreFailsClosedWithoutDatabase(t *testing.T) {
	store := NewReadinessStore(nil, nil)
	_, _, err := store.PublishQualificationRetentionPolicy(context.Background(), contracts.QualificationRetentionPolicyRequest{}, time.Now().UTC())
	if err == nil {
		t.Fatal("retention policy publication succeeded without a database")
	}
}

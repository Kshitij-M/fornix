package server

import (
	"context"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/credentials"
)

func TestResolveOpenAILeaseResolverFailsClosedForProduction(t *testing.T) {
	if _, err := resolveOpenAILeaseResolver(nil, ServerDependencies{}, true); err == nil {
		t.Fatal("production OpenAI composition accepted missing credential authority")
	}

	resolver := credentials.LeaseResolverFunc{
		AcquireFunc: func(context.Context, string, string, string, time.Duration) (credentials.Lease, error) {
			return credentials.Lease{}, credentials.ErrLeaseInvalid
		},
		ReleaseFunc:       func(context.Context, credentials.Lease) error { return nil },
		ValidateLeaseFunc: func(context.Context, credentials.Lease) error { return credentials.ErrLeaseInvalid },
	}
	resolved, err := resolveOpenAILeaseResolver(nil, ServerDependencies{OpenAILeaseResolver: resolver}, true)
	if err != nil || resolved == nil {
		t.Fatalf("injected production lease authority was rejected: resolver=%v err=%v", resolved, err)
	}
}

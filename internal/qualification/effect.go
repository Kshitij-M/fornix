package qualification

import (
	"context"
	"fmt"

	"github.com/omaveda/fornix/internal/contracts"
)

// EffectAuthorityProbe is supplied by an authority-owned integration harness.
// The probe may use disposable PostgreSQL and a fake provider, but it must not
// discover credentials or deployment state implicitly.
type EffectAuthorityProbe func(context.Context) (contracts.EffectAuthorityObservation, error)

// RunEffectAuthorityProbe validates and converts one redacted dispatcher
// observation into the common qualification case. Provider errors are returned
// to the runner for bounded classification; their text is never persisted.
func RunEffectAuthorityProbe(ctx context.Context, probe EffectAuthorityProbe) (contracts.QualificationCase, error) {
	if ctx == nil {
		return contracts.QualificationCase{}, fmt.Errorf("effect qualification context is nil")
	}
	if probe == nil {
		return contracts.QualificationCase{}, fmt.Errorf("effect qualification probe is nil")
	}
	observation, err := probe(ctx)
	if err != nil {
		return contracts.QualificationCase{}, err
	}
	return observation.QualificationCase()
}

// NewEffectAuthorityCheck creates an explicit external qualification check.
// The portable runner blocks it unless AllowExternalChecks is true; the
// default CLI never enables that option.
func NewEffectAuthorityCheck(probe EffectAuthorityProbe) Check {
	return Check{
		Name:     "effect-authority-dispatch",
		Version:  "1",
		Category: contracts.QualificationCategoryAuthority,
		Offline:  false,
		Run: func(ctx context.Context) (contracts.QualificationCase, error) {
			return RunEffectAuthorityProbe(ctx, probe)
		},
	}
}

package sandboxrunner

import (
	"context"
	"errors"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	// ErrSandboxImageUnavailable reports that the local Engine could not
	// inspect the expected image. The underlying Engine error is intentionally
	// discarded because it may disclose daemon endpoints or image references.
	ErrSandboxImageUnavailable = errors.New("sandbox local image is unavailable")
	// ErrSandboxImageIdentityMismatch reports a local image ID or platform that
	// differs from the sealed and qualified OCI plan.
	ErrSandboxImageIdentityMismatch = errors.New("sandbox local image identity mismatch")
)

// LocalImageInspection contains only the image identity fields required for
// pre-create policy verification. ImageID must be the full local config/image
// ID, not a tag, short ID, registry digest, or manifest/index digest.
type LocalImageInspection struct {
	ImageID  string
	Platform contracts.SandboxImagePlatform
}

// LocalImageInspector is intentionally inspection-only: it has no pull,
// import, build, tag lookup, or mutable-name resolution operation. A concrete
// Engine adapter must implement it with a local ID inspection and must call
// the plan verifier before creating a container.
type LocalImageInspector interface {
	InspectLocalImage(context.Context, string) (LocalImageInspection, error)
}

// VerifyLocalImage checks inspected Engine metadata against the immutable
// image identity in this sealed plan. It performs no Engine mutation and does
// not expose the rejected ID, platform, host, or underlying Engine error.
func (p OCIContainerPlan) VerifyLocalImage(ctx context.Context, inspector LocalImageInspector) error {
	if ctx == nil || inspector == nil {
		return ErrSandboxImageUnavailable
	}
	snapshot, err := p.engineSnapshot()
	if err != nil {
		return ErrContainerPlanInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inspected, err := inspector.InspectLocalImage(ctx, snapshot.imageDigest)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrSandboxImageUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validSHA256Digest(inspected.ImageID) || inspected.ImageID != snapshot.imageDigest {
		return ErrSandboxImageIdentityMismatch
	}
	platform := inspected.Platform
	if platform.Normalize() != nil || platform != snapshot.imagePlatform {
		return ErrSandboxImageIdentityMismatch
	}
	return nil
}

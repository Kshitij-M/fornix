package sandboxrunner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

type testLocalImageInspector struct {
	result  LocalImageInspection
	err     error
	called  int
	imageID string
}

func (i *testLocalImageInspector) InspectLocalImage(_ context.Context, imageID string) (LocalImageInspection, error) {
	i.called++
	i.imageID = imageID
	return i.result, i.err
}

func TestVerifyLocalImageRequiresExactIDAndPlatform(t *testing.T) {
	plan, _ := validTestOCIPlan(t)
	inspector := &testLocalImageInspector{result: LocalImageInspection{
		ImageID: plan.imageDigest, Platform: plan.imagePlatform,
	}}
	if err := plan.VerifyLocalImage(context.Background(), inspector); err != nil {
		t.Fatalf("exact local image rejected: %v", err)
	}
	if inspector.called != 1 || inspector.imageID != plan.imageDigest {
		t.Fatalf("inspector calls=%d imageID=%q, want one exact full local ID", inspector.called, inspector.imageID)
	}
}

func TestVerifyLocalImageFailsClosedBeforeAnyCreateForUntrustedMetadata(t *testing.T) {
	plan, _ := validTestOCIPlan(t)
	tests := []struct {
		name       string
		result     LocalImageInspection
		inspectErr error
		wantErr    error
	}{
		{
			name:       "missing image",
			inspectErr: errors.New("daemon at unix:///private/daemon.sock: image not found"),
			wantErr:    ErrSandboxImageUnavailable,
		},
		{
			name:    "short or mutable ID",
			result:  LocalImageInspection{ImageID: "sha256:abc", Platform: plan.imagePlatform},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
		{
			name:    "different full image ID",
			result:  LocalImageInspection{ImageID: "sha256:" + strings.Repeat("b", 64), Platform: plan.imagePlatform},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
		{
			name:    "tag reference is not an image ID",
			result:  LocalImageInspection{ImageID: "trusted/tool:latest", Platform: plan.imagePlatform},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
		{
			name:    "wrong OS",
			result:  LocalImageInspection{ImageID: plan.imageDigest, Platform: contracts.SandboxImagePlatform{OS: "windows", Architecture: "amd64"}},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
		{
			name:    "wrong architecture",
			result:  LocalImageInspection{ImageID: plan.imageDigest, Platform: contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
		{
			name:    "wrong variant",
			result:  LocalImageInspection{ImageID: plan.imageDigest, Platform: contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64", Variant: "v3"}},
			wantErr: ErrSandboxImageIdentityMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inspector := &testLocalImageInspector{result: test.result, err: test.inspectErr}
			err := plan.VerifyLocalImage(context.Background(), inspector)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("VerifyLocalImage() error=%v, want %v", err, test.wantErr)
			}
			if inspector.called != 1 || inspector.imageID != plan.imageDigest {
				t.Fatalf("inspector calls=%d imageID=%q", inspector.called, inspector.imageID)
			}
			if strings.Contains(err.Error(), "private/daemon.sock") || strings.Contains(err.Error(), "trusted/tool") {
				t.Fatalf("unsafe Engine detail escaped verifier: %v", err)
			}
		})
	}
}

func TestVerifyLocalImageRejectsInvalidPlanWithoutInspecting(t *testing.T) {
	plan, _ := validTestOCIPlan(t)
	plan.imageDigest = "registry.example/tool:latest"
	inspector := &testLocalImageInspector{}
	if err := plan.VerifyLocalImage(context.Background(), inspector); !errors.Is(err, ErrContainerPlanInvalid) {
		t.Fatalf("invalid plan error=%v", err)
	}
	if inspector.called != 0 {
		t.Fatalf("invalid plan reached image inspector %d times", inspector.called)
	}
}

func TestVerifyLocalImageHonorsCancellationWithoutInspecting(t *testing.T) {
	plan, _ := validTestOCIPlan(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inspector := &testLocalImageInspector{}
	if err := plan.VerifyLocalImage(ctx, inspector); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled verifier error=%v", err)
	}
	if inspector.called != 0 {
		t.Fatalf("cancelled verifier inspected image %d times", inspector.called)
	}
}

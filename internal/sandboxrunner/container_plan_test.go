package sandboxrunner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func validTestOCIPlan(t *testing.T) (OCIContainerPlan, string) {
	t.Helper()
	_, mount, reference, root := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	plan, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
	if err != nil {
		t.Fatalf("build OCI plan: %v", err)
	}
	t.Cleanup(func() { _ = mount.Close() })
	return plan, root
}

func TestBuildOCIContainerPlanIsDeterministicAndHardBounded(t *testing.T) {
	_, mount, reference, _ := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	plan, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
	if err != nil {
		t.Fatalf("build OCI plan: %v", err)
	}
	other, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
	if err != nil {
		t.Fatalf("rebuild OCI plan: %v", err)
	}
	if plan.calculatedHash() != other.calculatedHash() || plan.name != other.name {
		t.Fatal("identical execution identity did not produce a stable OCI plan")
	}
	if plan.name != request.Execution.RuntimeName() || plan.name != contracts.SandboxRuntimeNameFromHash(plan.executionHash) {
		t.Fatalf("OCI plan name=%q does not use canonical execution identity name %q", plan.name, request.Execution.RuntimeName())
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
	if plan.imageDigest != "sha256:"+strings.Repeat("a", 64) || plan.executable != "/usr/bin/git" ||
		strings.Join(plan.arguments, " ") != "show notes.txt" || plan.containerUser != "501:20" ||
		plan.networkMode != "none" || plan.privileged || !plan.readOnlyRootFS ||
		plan.workspaceTarget != "/workspace" || !plan.workspaceReadOnly || plan.workspaceRecursive ||
		plan.nanoCPUs != 500_000_000 || plan.memoryBytes != 64<<20 || plan.pidsLimit != 32 ||
		plan.scratchSizeBytes != 8<<20 || plan.timeout.Milliseconds() != 5000 || len(plan.environment) != 0 {
		t.Fatalf("plan does not map the admitted profile into fixed OCI controls: %#v", plan)
	}
	if !equalStrings(plan.droppedCapabilities, []string{"ALL"}) || !equalStrings(plan.securityOptions, []string{"no-new-privileges:true"}) {
		t.Fatalf("capability/security controls are not fixed: %#v %#v", plan.droppedCapabilities, plan.securityOptions)
	}
	for _, value := range plan.labels {
		if strings.Contains(value, "/") || strings.Contains(value, "notes.txt") || strings.Contains(value, "show") {
			t.Fatalf("runtime label leaked path or argv content: %q", value)
		}
	}
	if rendered := fmt.Sprintf("%#v", plan); strings.Contains(rendered, plan.workspaceSource) || strings.Contains(rendered, "notes.txt") {
		t.Fatalf("Go diagnostic string exposed sensitive plan fields: %s", rendered)
	}
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	logger.Info("plan", slog.Any("container", plan))
	if strings.Contains(logOutput.String(), plan.workspaceSource) || strings.Contains(logOutput.String(), "notes.txt") {
		t.Fatalf("structured logging exposed sensitive plan fields: %s", logOutput.String())
	}
	if _, err := json.Marshal(plan); err == nil {
		t.Fatal("plan must not serialize host paths and argv")
	}
	snapshot, err := plan.engineSnapshot()
	if err != nil {
		t.Fatalf("snapshot engine plan: %v", err)
	}
	snapshot.arguments[1] = "snapshot-mutation.txt"
	snapshot.labels[ociLabelPrefix+"tool-request-hash"] = "snapshot-mutation"
	if plan.arguments[1] != "notes.txt" || plan.labels[ociLabelPrefix+"tool-request-hash"] == "snapshot-mutation" {
		t.Fatal("mutating the detached Engine snapshot changed the sealed plan")
	}
}

func TestOCIContainerPlanRejectsSecurityRelaxationAndMutation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*OCIContainerPlan)
	}{
		{name: "network", change: func(p *OCIContainerPlan) { p.networkMode = "bridge" }},
		{name: "privileged", change: func(p *OCIContainerPlan) { p.privileged = true }},
		{name: "root filesystem writable", change: func(p *OCIContainerPlan) { p.readOnlyRootFS = false }},
		{name: "workspace writable", change: func(p *OCIContainerPlan) { p.workspaceReadOnly = false }},
		{name: "recursive bind", change: func(p *OCIContainerPlan) { p.workspaceRecursive = true }},
		{name: "capability added", change: func(p *OCIContainerPlan) { p.droppedCapabilities = nil }},
		{name: "security option removed", change: func(p *OCIContainerPlan) { p.securityOptions = nil }},
		{name: "root user", change: func(p *OCIContainerPlan) { p.containerUser = "0:0" }},
		{name: "image tag", change: func(p *OCIContainerPlan) { p.imageDigest = "ubuntu:latest" }},
		{name: "platform drift", change: func(p *OCIContainerPlan) {
			p.imagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
		}},
		{name: "label identity", change: func(p *OCIContainerPlan) {
			p.labels = cloneStringMap(p.labels)
			p.labels[ociLabelPrefix+"execution-hash"] = "forged"
		}},
		{name: "runtime name", change: func(p *OCIContainerPlan) { p.name = "fornix-sbx-" + p.executionHash[:32] }},
		{name: "extra label", change: func(p *OCIContainerPlan) {
			p.labels = cloneStringMap(p.labels)
			p.labels[ociLabelPrefix+"extra"] = "argv: secret"
		}},
		{name: "argv mutation", change: func(p *OCIContainerPlan) {
			p.arguments = append([]string(nil), p.arguments...)
			p.arguments[1] = "outside.txt"
		}},
		{name: "larger scratch", change: func(p *OCIContainerPlan) { p.scratchSizeBytes *= 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, _ := validTestOCIPlan(t)
			test.change(&plan)
			if err := plan.Validate(); err == nil {
				t.Fatal("mutated plan was accepted")
			}
		})
	}
}

func TestBuildOCIContainerPlanRejectsRootRunnerIdentity(t *testing.T) {
	_, mount, reference, _ := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	if _, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 0, 20); err != ErrContainerPlanInvalid {
		t.Fatalf("root runner identity error = %v, want ErrContainerPlanInvalid", err)
	}
	if _, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 0); err != ErrContainerPlanInvalid {
		t.Fatalf("root group identity error = %v, want ErrContainerPlanInvalid", err)
	}
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
}

package tool

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

// PolicyDecision is the selected bounded execution mode for one request.
type PolicyDecision struct {
	Rule contracts.ToolPolicyRule
	Mode string
}

// Policy is an immutable ordered policy set. An empty set denies every tool.
type Policy struct {
	mu    sync.RWMutex
	rules []contracts.ToolPolicyRule
}

// NewPolicy validates and deterministically orders policy rules. The empty
// policy denies all execution.
func NewPolicy(rules []contracts.ToolPolicyRule) (*Policy, error) {
	copyRules := make([]contracts.ToolPolicyRule, len(rules))
	for i, rule := range rules {
		copyRules[i] = rule
		if err := copyRules[i].Normalize(); err != nil {
			return nil, fmt.Errorf("policy rule %d: %w", i, err)
		}
	}
	sort.Slice(copyRules, func(i, j int) bool {
		if copyRules[i].Priority != copyRules[j].Priority {
			return copyRules[i].Priority > copyRules[j].Priority
		}
		si, sj := policySpecificity(copyRules[i]), policySpecificity(copyRules[j])
		if si != sj {
			return si > sj
		}
		ri, rj := policyModeRank(copyRules[i].Mode), policyModeRank(copyRules[j].Mode)
		if ri != rj {
			return ri > rj
		}
		return copyRules[i].ID < copyRules[j].ID
	})
	return &Policy{rules: copyRules}, nil
}

// Evaluate applies workspace, actor, entity, argv, environment, and workdir
// constraints. No matching rule is an authorization failure.
func (p *Policy) Evaluate(req contracts.ToolRequest, def contracts.ToolDefinition) (PolicyDecision, error) {
	if p == nil {
		return PolicyDecision{}, fmt.Errorf("%w: no policy configured", ErrUnauthorized)
	}
	p.mu.RLock()
	rules := append([]contracts.ToolPolicyRule(nil), p.rules...)
	p.mu.RUnlock()
	for _, rule := range rules {
		if !rule.Enabled || !ruleMatches(rule, req, def) {
			continue
		}
		return PolicyDecision{Rule: rule, Mode: rule.Mode}, nil
	}
	return PolicyDecision{}, fmt.Errorf("%w: no matching policy for tool %s", ErrUnauthorized, req.ToolID)
}

// RegisterWorkspaceTool adds the narrowly scoped read-only repository rule
// used by the reference workflow. Rules remain in-memory admission policy;
// the durable workspace/tool authorization is still enforced by the
// authenticated request and task fences.
func (p *Policy) RegisterWorkspaceTool(workspaceID, toolID, capability, root string) error {
	if p == nil {
		return fmt.Errorf("tool policy is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	toolID = strings.TrimSpace(toolID)
	capability = strings.TrimSpace(capability)
	root = strings.TrimSpace(root)
	if workspaceID == "" || toolID == "" || capability == "" || root == "" {
		return fmt.Errorf("workspace_id, tool_id, capability, and root are required")
	}
	rule := contracts.ToolPolicyRule{
		ID:          "workspace-repository-read-" + workspaceID,
		Priority:    200,
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		Capability:  capability,
		Mode:        contracts.ToolModeAutomatic,
		Enabled:     true,
		WorkdirRoot: root,
		Sandbox:     contracts.SandboxProfile{ReadOnlyWorkdir: true, AllowNetwork: false, MaxStdoutBytes: 128 << 10, MaxStderrBytes: 128 << 10, TimeoutMS: 5000},
	}
	if err := rule.Normalize(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rules {
		if p.rules[i].ID == rule.ID {
			p.rules[i] = rule
			return nil
		}
	}
	p.rules = append(p.rules, rule)
	sort.Slice(p.rules, func(i, j int) bool {
		if p.rules[i].Priority != p.rules[j].Priority {
			return p.rules[i].Priority > p.rules[j].Priority
		}
		return p.rules[i].ID < p.rules[j].ID
	})
	return nil
}

func ruleMatches(rule contracts.ToolPolicyRule, req contracts.ToolRequest, def contracts.ToolDefinition) bool {
	if rule.WorkspaceID != req.WorkspaceID || rule.ToolID != req.ToolID {
		return false
	}
	requestedCapability := strings.ToLower(strings.TrimSpace(req.Capability))
	definitionCapability := strings.ToLower(strings.TrimSpace(def.Capability))
	if requestedCapability != "" && requestedCapability != definitionCapability {
		return false
	}
	if rule.Capability != "" && rule.Capability != firstNonEmpty(requestedCapability, definitionCapability) {
		return false
	}
	if rule.ActorID != "" && rule.ActorID != strings.TrimSpace(req.Actor.ID) {
		return false
	}
	if rule.TaskID != "" && (req.Task == nil || rule.TaskID != req.Task.ID) {
		return false
	}
	if rule.SessionID != "" && (req.Session == nil || rule.SessionID != req.Session.ID) {
		return false
	}
	if !prefixMatches(req.Argv, rule.ArgvPrefix) {
		return false
	}
	for key := range req.Environment {
		if !contains(rule.AllowedEnvKeys, key) {
			return false
		}
	}
	if rule.WorkdirRoot != "" && !withinRoot(req.Workdir, rule.WorkdirRoot) {
		return false
	}
	return true
}

func policySpecificity(rule contracts.ToolPolicyRule) int {
	n := 0
	if rule.ActorID != "" {
		n++
	}
	if rule.TaskID != "" {
		n++
	}
	if rule.SessionID != "" {
		n++
	}
	if rule.Capability != "" {
		n++
	}
	if len(rule.ArgvPrefix) > 0 {
		n++
	}
	if len(rule.AllowedEnvKeys) > 0 {
		n++
	}
	if rule.WorkdirRoot != "" {
		n++
	}
	return n
}
func policyModeRank(mode string) int {
	if mode == contracts.ToolModeDenied {
		return 2
	}
	return 1
}
func prefixMatches(argv, prefix []string) bool {
	if len(prefix) > len(argv) {
		return false
	}
	for i := range prefix {
		if prefix[i] != argv[i] {
			return false
		}
	}
	return true
}
func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
func withinRoot(path, root string) bool {
	if path == "" {
		return true
	}
	absPath, err1 := filepath.Abs(path)
	absRoot, err2 := filepath.Abs(root)
	if err1 != nil || err2 != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

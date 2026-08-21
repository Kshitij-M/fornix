package tool

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

// Registry is an explicit, in-process capability catalog. Registration is
// deterministic and collision-safe; it is not an authorization decision.
type Registry struct {
	mu      sync.RWMutex
	tools   map[string]contracts.ToolDefinition
	aliases map[string]string
}

func NewRegistry() *Registry {
	return &Registry{tools: map[string]contracts.ToolDefinition{}, aliases: map[string]string{}}
}

func (r *Registry) Register(def contracts.ToolDefinition) error {
	if r == nil {
		return fmt.Errorf("tool registry is nil")
	}
	if err := def.Normalize(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.ToLower(def.ID)
	if _, exists := r.tools[key]; exists {
		return fmt.Errorf("tool %q is already registered", def.ID)
	}
	for _, alias := range []string{def.ID, def.Name} {
		alias = strings.ToLower(strings.TrimSpace(alias))
		if existing, ok := r.aliases[alias]; ok {
			return fmt.Errorf("tool alias %q already resolves to %q", alias, existing)
		}
	}
	r.tools[key] = cloneDefinition(def)
	r.aliases[strings.ToLower(def.ID)] = key
	r.aliases[strings.ToLower(def.Name)] = key
	return nil
}

func (r *Registry) Lookup(name string) (contracts.ToolDefinition, bool) {
	if r == nil {
		return contracts.ToolDefinition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.aliases[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return contracts.ToolDefinition{}, false
	}
	def, ok := r.tools[key]
	return cloneDefinition(def), ok
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.tools))
	for key := range r.tools {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func cloneDefinition(def contracts.ToolDefinition) contracts.ToolDefinition {
	def.ArgvPrefix = append([]string(nil), def.ArgvPrefix...)
	def.AllowedEnvKeys = append([]string(nil), def.AllowedEnvKeys...)
	return def
}

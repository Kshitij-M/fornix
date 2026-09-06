package runtime

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
)

// ParseStatus parses Docker Compose's one-JSON-object-per-line status format.
// It is intentionally strict so machine-readable CLI output never silently
// treats a human diagnostic as service state.
func ParseStatus(raw string) ([]map[string]any, error) {
	var services []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var service map[string]any
		if err := json.Unmarshal([]byte(line), &service); err != nil {
			return nil, fmt.Errorf("parse compose status: %w", err)
		}
		services = append(services, service)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan compose status: %w", err)
	}
	return services, nil
}

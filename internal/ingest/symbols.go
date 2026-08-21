package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

type symbolPattern struct {
	kind, language string
	re             *regexp.Regexp
}

var symbolPatterns = map[string][]symbolPattern{
	"go": {{"function", "go", regexp.MustCompile(`^\s*func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(`)}, {"type", "go", regexp.MustCompile(`^\s*type\s+([A-Za-z_][A-Za-z0-9_]*)\b`)}},
	"py": {{"function", "python", regexp.MustCompile(`^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)}, {"class", "python", regexp.MustCompile(`^\s*class\s+([A-Za-z_][A-Za-z0-9_]*)\b`)}},
	"js": {{"function", "javascript", regexp.MustCompile(`^\s*(?:export\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(`)}, {"class", "javascript", regexp.MustCompile(`^\s*(?:export\s+)?class\s+([A-Za-z_$][A-Za-z0-9_$]*)\b`)}},
	"ts": {{"function", "typescript", regexp.MustCompile(`^\s*(?:export\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(`)}, {"class", "typescript", regexp.MustCompile(`^\s*(?:export\s+)?class\s+([A-Za-z_$][A-Za-z0-9_$]*)\b`)}},
	"rs": {{"function", "rust", regexp.MustCompile(`^\s*(?:pub\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)}, {"struct", "rust", regexp.MustCompile(`^\s*(?:pub\s+)?struct\s+([A-Za-z_][A-Za-z0-9_]*)\b`)}, {"enum", "rust", regexp.MustCompile(`^\s*(?:pub\s+)?enum\s+([A-Za-z_][A-Za-z0-9_]*)\b`)}},
}

func Symbols(path string, data []byte) []contracts.IngestSymbol {
	ext := filepath.Ext(path)
	if len(ext) > 0 {
		ext = ext[1:]
	}
	patterns := symbolPatterns[strings.ToLower(ext)]
	if len(patterns) == 0 {
		return nil
	}
	lines := strings.Split(string(data), "\n")
	out := make([]contracts.IngestSymbol, 0)
	for lineNo, line := range lines {
		for _, pattern := range patterns {
			match := pattern.re.FindStringSubmatch(line)
			if len(match) < 2 {
				continue
			}
			h := sha256.Sum256([]byte(path + "\x00" + pattern.kind + "\x00" + match[1] + "\x00" + line))
			out = append(out, contracts.IngestSymbol{FilePath: path, SymbolName: match[1], SymbolKind: pattern.kind, Language: pattern.language, LineStart: lineNo + 1, LineEnd: lineNo + 1, Signature: strings.TrimSpace(line), ContentHash: hex.EncodeToString(h[:])})
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LineStart != out[j].LineStart {
			return out[i].LineStart < out[j].LineStart
		}
		if out[i].SymbolName != out[j].SymbolName {
			return out[i].SymbolName < out[j].SymbolName
		}
		return out[i].SymbolKind < out[j].SymbolKind
	})
	return out
}

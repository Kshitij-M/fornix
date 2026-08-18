package retrieval

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

type compileInput struct {
	RequestHash string
	WorkspaceID string
	Items       []contracts.ContextItem
	Budget      contracts.RetrievalBudget
}

// Compile applies all hard budgets after stage fusion. It is pure: the same
// candidate ordering and budget always produce the same pack and content hash.
func Compile(requestHash, workspaceID string, budget contracts.RetrievalBudget, candidates []contracts.ContextItem) contracts.ContextPack {
	ordered := append([]contracts.ContextItem(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Score != ordered[j].Score {
			return ordered[i].Score > ordered[j].Score
		}
		if ordered[i].Kind != ordered[j].Kind {
			return ordered[i].Kind < ordered[j].Kind
		}
		return ordered[i].SourceReference < ordered[j].SourceReference
	})

	items := make([]contracts.ContextItem, 0, minInt(len(ordered), budget.MaxItems))
	totalBytes, totalTokens := 0, 0
	truncated := false
	for _, source := range ordered {
		if len(items) >= budget.MaxItems {
			break
		}
		if strings.TrimSpace(source.WorkspaceID) != workspaceID {
			continue
		}
		fullText := source.Text
		fullBytes := len([]byte(fullText))
		fullTokens := contracts.EstimateTokens(fullText)
		remainingBytes := budget.MaxBytes - totalBytes
		remainingTokens := budget.MaxTokens - totalTokens
		if remainingBytes <= 0 || remainingTokens <= 0 {
			break
		}
		item := source
		item.OriginalBytes = fullBytes
		item.OriginalTokens = fullTokens
		if fullBytes > remainingBytes || fullTokens > remainingTokens {
			item.Text = boundedPrefix(fullText, remainingBytes, remainingTokens)
			item.Truncated = item.Text != fullText
			truncated = truncated || item.Truncated
		}
		if item.Text == "" {
			continue
		}
		itemBytes := len([]byte(item.Text))
		itemTokens := contracts.EstimateTokens(item.Text)
		if itemBytes > remainingBytes || itemTokens > remainingTokens {
			continue
		}
		totalBytes += itemBytes
		totalTokens += itemTokens
		items = append(items, item)
	}

	pack := contracts.ContextPack{
		SchemaVersion: contracts.RetrievalSchemaVersion,
		WorkspaceID:   workspaceID,
		RequestHash:   requestHash,
		Items:         items,
		TotalBytes:    totalBytes,
		TotalTokens:   totalTokens,
		Truncated:     truncated,
		Abstained:     len(items) == 0,
	}
	pack.ContentHash = contentHash(pack, budget)
	return pack
}

func boundedPrefix(text string, maxBytes, maxTokens int) string {
	if text == "" || maxBytes <= 0 || maxTokens <= 0 {
		return ""
	}
	if len([]byte(text)) <= maxBytes && contracts.EstimateTokens(text) <= maxTokens {
		return text
	}
	runes := []rune(text)
	low, high := 0, len(runes)
	for low < high {
		mid := low + (high-low+1)/2
		candidate := string(runes[:mid])
		if len([]byte(candidate)) <= maxBytes && contracts.EstimateTokens(candidate) <= maxTokens {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if low == 0 {
		return ""
	}
	return string(runes[:low])
}

func contentHash(pack contracts.ContextPack, budget contracts.RetrievalBudget) string {
	input := compileInput{
		RequestHash: pack.RequestHash,
		WorkspaceID: pack.WorkspaceID,
		Items:       pack.Items,
		Budget:      budget,
	}
	b, err := json.Marshal(input)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func validUTF8(text string) bool { return utf8.ValidString(text) }

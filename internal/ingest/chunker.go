// Package ingest admits repository sources into Fornix's verifiable AI work
// path and turns stable file snapshots into bounded chunks and optional
// lightweight symbol records. It preserves source identity so later context
// and work results can be traced back to a known repository snapshot.
package ingest

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ChunkWindow identifies a deterministic rune and line range within one
// source file.
type ChunkWindow struct {
	Index     int
	RuneStart int
	RuneEnd   int
	LineStart int
	LineEnd   int
	Text      string
}

// Chunk splits valid UTF-8 source into overlapping deterministic windows.
func Chunk(data []byte, size, overlap int) ([]ChunkWindow, error) {
	if size <= 0 {
		size = 4096
	}
	if overlap < 0 || overlap >= size {
		return nil, fmt.Errorf("invalid chunk overlap")
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("chunk input is not UTF-8")
	}
	runes := []rune(string(data))
	if len(runes) == 0 {
		return nil, nil
	}
	step := size - overlap
	chunks := make([]ChunkWindow, 0, (len(runes)+step-1)/step)
	for start, index := 0, 0; start < len(runes); start, index = start+step, index+1 {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		text := string(runes[start:end])
		lineStart := 1 + strings.Count(string(runes[:start]), "\n")
		lineEnd := lineStart + strings.Count(text, "\n")
		chunks = append(chunks, ChunkWindow{Index: index, RuneStart: start, RuneEnd: end, LineStart: lineStart, LineEnd: lineEnd, Text: text})
		if end == len(runes) {
			break
		}
	}
	return chunks, nil
}

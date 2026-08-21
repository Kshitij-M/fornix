package ingest

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type ChunkWindow struct {
	Index     int
	RuneStart int
	RuneEnd   int
	LineStart int
	LineEnd   int
	Text      string
}

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

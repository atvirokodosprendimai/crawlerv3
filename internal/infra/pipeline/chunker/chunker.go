// Package chunker splits plain text into overlapping word chunks.
package chunker

import (
	"strings"
	"unicode"
)

// Config controls Split behavior.
type Config struct {
	WordsPerChunk int // default 400
	OverlapWords  int // default 50
}

// Defaults returns a sensible Config.
func Defaults() Config { return Config{WordsPerChunk: 400, OverlapWords: 50} }

// Chunk is a slice of the input text.
type Chunk struct {
	Index      int
	Text       string
	WordCount  int
}

// Split returns overlapping chunks built from word boundaries.
func Split(text string, cfg Config) []Chunk {
	if cfg.WordsPerChunk <= 0 {
		cfg.WordsPerChunk = 400
	}
	if cfg.OverlapWords < 0 || cfg.OverlapWords >= cfg.WordsPerChunk {
		cfg.OverlapWords = 50
	}
	words := splitWords(text)
	if len(words) == 0 {
		return nil
	}
	var out []Chunk
	step := cfg.WordsPerChunk - cfg.OverlapWords
	if step <= 0 {
		step = cfg.WordsPerChunk
	}
	idx := 0
	for start := 0; start < len(words); start += step {
		end := start + cfg.WordsPerChunk
		if end > len(words) {
			end = len(words)
		}
		seg := strings.Join(words[start:end], " ")
		out = append(out, Chunk{Index: idx, Text: seg, WordCount: end - start})
		idx++
		if end == len(words) {
			break
		}
	}
	return out
}

func splitWords(s string) []string {
	var words []string
	var b strings.Builder
	for _, r := range s {
		if unicode.IsSpace(r) {
			if b.Len() > 0 {
				words = append(words, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteRune(r)
	}
	if b.Len() > 0 {
		words = append(words, b.String())
	}
	return words
}

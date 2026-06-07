// Package tiktoken wraps github.com/pkoukk/tiktoken-go behind the small
// chunker.Tokenizer interface so the chunker can stay infra-agnostic.
package tiktoken

import (
	"fmt"

	"github.com/pkoukk/tiktoken-go"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
)

// Tokenizer is a chunker.Tokenizer backed by a tiktoken BPE encoding.
type Tokenizer struct {
	name string
	enc  *tiktoken.Tiktoken
}

// New loads the named BPE encoding. Supported names match tiktoken-go's
// registry: "cl100k_base" (GPT-3.5/4 family), "p50k_base", "r50k_base", etc.
// cl100k_base is the standard default — it covers English and Western
// scripts well; non-Latin alphabets are over-segmented compared to a model's
// own tokenizer, which the chunker absorbs via conservative size defaults.
func New(name string) (*Tokenizer, error) {
	enc, err := tiktoken.GetEncoding(name)
	if err != nil {
		return nil, fmt.Errorf("tiktoken: load %q: %w", name, err)
	}
	return &Tokenizer{name: name, enc: enc}, nil
}

// MustNew is New that panics on error. Use only at process startup.
func MustNew(name string) *Tokenizer {
	t, err := New(name)
	if err != nil {
		panic(err)
	}
	return t
}

// Name returns the encoding name passed to New.
func (t *Tokenizer) Name() string { return t.name }

// Encode returns the token IDs for s. Returns nil for an empty string.
// Special tokens are not interpreted — all input is treated as ordinary text.
func (t *Tokenizer) Encode(s string) []int {
	if s == "" {
		return nil
	}
	return t.enc.Encode(s, nil, nil)
}

// Decode returns the string for the given token IDs.
func (t *Tokenizer) Decode(ids []int) string {
	if len(ids) == 0 {
		return ""
	}
	return t.enc.Decode(ids)
}

// Compile-time check.
var _ chunker.Tokenizer = (*Tokenizer)(nil)

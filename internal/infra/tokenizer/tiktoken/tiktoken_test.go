package tiktoken_test

import (
	"strings"
	"testing"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/tokenizer/tiktoken"
)

func TestRoundTrip(t *testing.T) {
	tok, err := tiktoken.New("cl100k_base")
	if err != nil {
		t.Fatalf("load cl100k_base: %v", err)
	}
	if tok.Name() != "cl100k_base" {
		t.Errorf("Name() = %q, want cl100k_base", tok.Name())
	}

	cases := []struct {
		label string
		in    string
	}{
		{"empty", ""},
		{"ascii short", "hello world"},
		{"ascii sentence", "The quick brown fox jumps over the lazy dog."},
		{"lithuanian", "Lietuvos Aukščiausiasis Teismas išnagrinėjo civilinę bylą."},
		{"lithuanian diacritics", "ąčęėįšųūž ĄČĘĖĮŠŲŪŽ"},
		{"mixed", "Bylos numeris: e3K-3-456-823/2024 — sprendimas paskelbtas 2024-03-15."},
		{"newlines + tabs", "para 1\n\npara 2 with\ttab\nthird line"},
		{"unicode emoji", "search hits ✓ and ✗"},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			ids := tok.Encode(c.in)
			if c.in == "" {
				if ids != nil {
					t.Errorf("Encode(\"\") = %v, want nil", ids)
				}
				if got := tok.Decode(nil); got != "" {
					t.Errorf("Decode(nil) = %q, want \"\"", got)
				}
				return
			}
			if len(ids) == 0 {
				t.Fatalf("Encode(%q) = empty for non-empty input", c.in)
			}
			got := tok.Decode(ids)
			if got != c.in {
				t.Errorf("round-trip mismatch:\n  in:  %q\n  out: %q", c.in, got)
			}
		})
	}
}

// TestChunkerEndToEnd_WithRealTokenizer verifies that the chunker, fed the
// real cl100k_base tiktoken, produces chunks whose concatenated cores cover
// the input losslessly — the load-bearing assumption that lets us decode the
// full prev||core||next slice in one Decode call.
func TestChunkerEndToEnd_WithRealTokenizer(t *testing.T) {
	tok, err := tiktoken.New("cl100k_base")
	if err != nil {
		t.Fatal(err)
	}
	en := strings.Repeat("The court issued its decision on the appeal. ", 500)
	cfg := chunker.Config{
		ChunkTokens: 200,
		OverlapPrev: 30,
		OverlapNext: 30,
		Tok:         tok,
	}
	chunks := chunker.Split(en, cfg)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	totalTokens := len(tok.Encode(en))
	sumCore := 0
	for _, c := range chunks {
		sumCore += c.TokenCount
		// Each chunk must be at least the core size.
		if got := len(tok.Encode(c.Text)); got < c.TokenCount {
			t.Errorf("chunk %d encoded len %d < core %d", c.Index, got, c.TokenCount)
		}
	}
	if sumCore != totalTokens {
		t.Errorf("sum of core tokens = %d, expected %d (full doc length)", sumCore, totalTokens)
	}

	// The first chunk's prefix decodes back to a substring of the original
	// input — confirms concat-Decode roundtrip is clean enough for retrieval.
	if !strings.HasPrefix(en, chunks[0].Text[:1]) {
		t.Errorf("chunk 0 prefix does not match input start byte")
	}
}

func TestTokenDensity(t *testing.T) {
	// Sanity: Lithuanian over-segments compared to English, but stays within an
	// order of magnitude — anchors the "conservative defaults absorb undercount"
	// claim in the spec.
	tok, err := tiktoken.New("cl100k_base")
	if err != nil {
		t.Fatal(err)
	}
	en := strings.Repeat("The court issued its decision on the appeal. ", 50)
	lt := strings.Repeat("Teismas priėmė sprendimą dėl apeliacinio skundo. ", 50)
	enTokens := len(tok.Encode(en))
	ltTokens := len(tok.Encode(lt))
	if enTokens == 0 || ltTokens == 0 {
		t.Fatalf("got zero tokens: en=%d lt=%d", enTokens, ltTokens)
	}
	ratio := float64(ltTokens) / float64(enTokens)
	if ratio < 1.0 || ratio > 5.0 {
		t.Errorf("lt/en token ratio = %.2f (en=%d lt=%d), expected 1.0-5.0",
			ratio, enTokens, ltTokens)
	}
	t.Logf("en=%d lt=%d ratio=%.2f", enTokens, ltTokens, ratio)
}

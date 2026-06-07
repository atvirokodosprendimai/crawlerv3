package chunker_test

import (
	"strings"
	"testing"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
)

// runeTok is a one-rune-per-token tokenizer. Decode is just string(runes),
// so Encode/Decode round-trip is lossless and concat-safe — every input byte
// keeps its position relative to its neighbors. Good enough to exercise the
// chunker's prev||core||next math without any BPE complexity.
type runeTok struct{}

func (runeTok) Name() string { return "fake-runes" }
func (runeTok) Encode(s string) []int {
	if s == "" {
		return nil
	}
	out := make([]int, 0, len(s))
	for _, r := range s {
		out = append(out, int(r))
	}
	return out
}
func (runeTok) Decode(ids []int) string {
	if len(ids) == 0 {
		return ""
	}
	rs := make([]rune, len(ids))
	for i, id := range ids {
		rs[i] = rune(id)
	}
	return string(rs)
}

// --- tests ----------------------------------------------------------------

func TestEmpty(t *testing.T) {
	cfg := chunker.Defaults()
	cfg.Tok = runeTok{}
	if got := chunker.Split("", cfg); got != nil {
		t.Errorf("Split(\"\") = %v, want nil", got)
	}
}

func TestPanicsWhenTokNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil Tok, got none")
		}
	}()
	chunker.Split("hello", chunker.Defaults())
}

func TestShortInput_SingleChunkNoOverlap(t *testing.T) {
	cfg := chunker.Config{ChunkTokens: 100, OverlapPrev: 20, OverlapNext: 20, Tok: runeTok{}}
	text := strings.Repeat("a", 50)
	got := chunker.Split(text, cfg)
	if len(got) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(got))
	}
	if got[0].TokenCount != 50 {
		t.Errorf("TokenCount = %d, want 50", got[0].TokenCount)
	}
	if got[0].Text != text {
		t.Errorf("single-chunk Text mismatch:\n  in:  %q\n  out: %q", text, got[0].Text)
	}
	if got[0].Index != 0 {
		t.Errorf("Index = %d, want 0", got[0].Index)
	}
}

func TestExactBoundary_TwoChunks(t *testing.T) {
	// Doc is 200 distinct runes a..z<repeat>. Chunk=100, prev=next=20.
	//   chunk 0: core=text[0:100], next=text[100:120]   → text[0:120]
	//   chunk 1: prev=text[80:100], core=text[100:200]  → text[80:200]
	cfg := chunker.Config{ChunkTokens: 100, OverlapPrev: 20, OverlapNext: 20, Tok: runeTok{}}
	text := distinctRunes(200)
	got := chunker.Split(text, cfg)
	if len(got) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(got))
	}
	rs := []rune(text)
	if got[0].Text != string(rs[0:120]) {
		t.Errorf("chunk 0 mismatch\n  want: %q\n  got:  %q", string(rs[0:120]), got[0].Text)
	}
	if got[1].Text != string(rs[80:200]) {
		t.Errorf("chunk 1 mismatch\n  want: %q\n  got:  %q", string(rs[80:200]), got[1].Text)
	}
	if got[0].TokenCount != 100 || got[1].TokenCount != 100 {
		t.Errorf("token counts: %d, %d (want 100, 100)", got[0].TokenCount, got[1].TokenCount)
	}
}

func TestSpecExample_10kTokens_2800_400_400(t *testing.T) {
	// From spec verification §1: 10000-token doc + Defaults (2800/400/400)
	// → 4 chunks with cores 2800, 2800, 2800, 1600.
	cfg := chunker.Defaults()
	cfg.Tok = runeTok{}
	text := distinctRunes(10000)
	rs := []rune(text)

	got := chunker.Split(text, cfg)
	if len(got) != 4 {
		t.Fatalf("want 4 chunks for 10000-token doc with cores 2800, got %d", len(got))
	}
	wantCores := []int{2800, 2800, 2800, 1600}
	for i, c := range got {
		if c.TokenCount != wantCores[i] {
			t.Errorf("chunk %d TokenCount = %d, want %d", i, c.TokenCount, wantCores[i])
		}
		if c.Index != i {
			t.Errorf("chunk %d Index = %d", i, c.Index)
		}
	}

	// Chunk 0: prev=∅,            core=rs[0:2800],     next=rs[2800:3200]
	if got[0].Text != string(rs[0:3200]) {
		t.Errorf("chunk 0 boundary mismatch")
	}
	// Chunk 1: prev=rs[2400:2800], core=rs[2800:5600],  next=rs[5600:6000]
	if got[1].Text != string(rs[2400:6000]) {
		t.Errorf("chunk 1 boundary mismatch")
	}
	// Chunk 3 (last): prev=rs[8000:8400], core=rs[8400:10000], next=∅
	if got[3].Text != string(rs[8000:10000]) {
		t.Errorf("chunk 3 boundary mismatch")
	}
}

func TestOverlapCappedAtChunkSize(t *testing.T) {
	cfg := chunker.Config{ChunkTokens: 50, OverlapPrev: 9999, OverlapNext: 9999, Tok: runeTok{}}
	got := chunker.Split(distinctRunes(150), cfg)
	if len(got) != 3 {
		t.Fatalf("want 3 chunks, got %d", len(got))
	}
}

func TestNegativeOverlapsClamped(t *testing.T) {
	cfg := chunker.Config{ChunkTokens: 50, OverlapPrev: -5, OverlapNext: -1, Tok: runeTok{}}
	got := chunker.Split(distinctRunes(120), cfg)
	if len(got) != 3 {
		t.Fatalf("want 3 chunks, got %d", len(got))
	}
	// With no overlap, sum of core sizes == total rune count == 120.
	sum := 0
	for _, c := range got {
		sum += c.TokenCount
	}
	if sum != 120 {
		t.Errorf("total core tokens = %d, want 120", sum)
	}
}

// distinctRunes returns a string of n distinct runes — guarantees every
// position has a unique token id under runeTok so boundary equality has
// no ambiguity.
func distinctRunes(n int) string {
	rs := make([]rune, n)
	for i := range rs {
		// Pick from the BMP private-use range so the test never collides
		// with anything resembling natural text.
		rs[i] = rune(0xE000 + i)
	}
	return string(rs)
}

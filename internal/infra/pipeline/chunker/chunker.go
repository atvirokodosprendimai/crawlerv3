// Package chunker splits plain text into overlapping token-space chunks
// ready to feed to an embedding model.
//
// Chunk shape is prev-overlap || core || next-overlap in tokens:
//
//	chunk 0:    [-]                | core 0 | next 0
//	chunk 1:    prev 0 (tail of c0)| core 1 | next 1 (head of c2 core)
//	...
//	chunk N-1:  prev N-2           | core N-1 | [-]
//
// Cores never share a token across chunks. Overlaps are pulled from the
// neighbor's core, not from the same chunk's core. Boundary chunks (first
// and last) have one-sided overlap.
package chunker

// Config controls Split behavior.
//
// All sizes are in tokens, not bytes or words. Tok is required; Split panics
// if Tok is nil, since silent fallback would mask wiring bugs at startup.
type Config struct {
	ChunkTokens int       // core size, default 2800
	OverlapPrev int       // tokens prepended from previous chunk, default 400
	OverlapNext int       // tokens appended from next chunk, default 400
	Tok         Tokenizer // tokenizer; required
}

// Defaults returns a registry-wide sensible Config.
//
// Tok is left nil — the caller MUST set Tok before passing this Config to
// Split. The intent: keep the chunker package free of any concrete tokenizer
// import so the dependency direction stays domain → infra.
func Defaults() Config {
	return Config{ChunkTokens: 2800, OverlapPrev: 400, OverlapNext: 400}
}

// Chunk is one piece of the input.
type Chunk struct {
	Index      int    // 0-based position in document order
	Text       string // prev-overlap || core || next-overlap, ready for embedding
	TokenCount int    // CORE token count — not the total Text length
}

// Split returns ordered chunks built in token space.
//
// Behavior contract:
//   - For an empty input string, returns nil.
//   - For an input shorter than ChunkTokens, returns one chunk whose Text is
//     the whole input and whose overlaps are empty.
//   - For longer input, returns ceil(len(tokens)/ChunkTokens) chunks where
//     chunk i's core is tokens[i*ChunkTokens : i*ChunkTokens+ChunkTokens]
//     (truncated at the last chunk), prev-overlap = last OverlapPrev tokens
//     of chunk i-1's core (empty for i=0), and next-overlap = first
//     OverlapNext tokens of chunk i+1's core (empty for the last chunk).
//   - TokenCount records the CORE size only.
//
// A zero ChunkTokens or negative overlaps fall back to Defaults; an overlap
// larger than ChunkTokens is silently capped to ChunkTokens.
func Split(text string, cfg Config) []Chunk {
	if cfg.Tok == nil {
		panic("chunker.Split: Config.Tok is nil — wire a Tokenizer at startup")
	}
	if text == "" {
		return nil
	}
	if cfg.ChunkTokens <= 0 {
		cfg.ChunkTokens = 2800
	}
	if cfg.OverlapPrev < 0 {
		cfg.OverlapPrev = 0
	}
	if cfg.OverlapNext < 0 {
		cfg.OverlapNext = 0
	}
	if cfg.OverlapPrev > cfg.ChunkTokens {
		cfg.OverlapPrev = cfg.ChunkTokens
	}
	if cfg.OverlapNext > cfg.ChunkTokens {
		cfg.OverlapNext = cfg.ChunkTokens
	}

	all := cfg.Tok.Encode(text)
	if len(all) == 0 {
		return nil
	}

	// Pre-compute core ranges so prev/next can grab from the right window.
	type rng struct{ start, end int }
	cores := make([]rng, 0, (len(all)/cfg.ChunkTokens)+1)
	for start := 0; start < len(all); start += cfg.ChunkTokens {
		end := start + cfg.ChunkTokens
		if end > len(all) {
			end = len(all)
		}
		cores = append(cores, rng{start, end})
	}

	out := make([]Chunk, len(cores))
	for i, c := range cores {
		lo, hi := c.start, c.end // boundary defaults — extended below if neighbors exist
		if i > 0 {
			p := cores[i-1]
			pl := p.end - cfg.OverlapPrev
			if pl < p.start {
				pl = p.start
			}
			lo = pl
		}
		if i < len(cores)-1 {
			n := cores[i+1]
			nh := n.start + cfg.OverlapNext
			if nh > n.end {
				nh = n.end
			}
			hi = nh
		}
		// Single Decode over the contiguous (prev||core||next) ID slice — many
		// tokenizers (e.g. tiktoken BPE) only round-trip cleanly when whole
		// concatenated regions are decoded together; splitting per region
		// would lose the inter-region byte that joined them in Encode.
		out[i] = Chunk{
			Index:      i,
			Text:       cfg.Tok.Decode(all[lo:hi]),
			TokenCount: c.end - c.start,
		}
	}
	return out
}

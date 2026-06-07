package chunker

// Tokenizer is the minimum surface chunker.Split needs to operate in token
// space. Implementations live in internal/infra/tokenizer/<name>/.
//
// Round-trip contract: for every byte sequence s,
//
//	Decode(Encode(s)) == s
//
// must hold up to whatever normalization the underlying BPE applies.
// Implementations that diverge (e.g. lossy normalization) MUST document the
// divergence — the chunker relies on Decode(Encode(core)) producing the text
// that lands in the stored chunk.Text payload.
type Tokenizer interface {
	// Name returns a stable identifier ("cl100k_base", "sentencepiece-bge-m3",
	// …) suitable for storing in the collections table and surfacing to operators.
	Name() string

	// Encode returns the token IDs for s. Returns nil for an empty string.
	Encode(s string) []int

	// Decode returns the string for the given token IDs. Decode(nil) == "".
	Decode(ids []int) string
}

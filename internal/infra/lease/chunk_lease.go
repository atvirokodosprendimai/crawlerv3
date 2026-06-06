package lease

import (
	"crypto/sha256"
	"time"
)

// SignChunk produces a lease for a chunk UUID by hashing the UUID to 32 bytes
// and reusing the standard Sign mechanism. Verify is symmetric (caller hashes
// the received chunk_id and compares to urlHash returned by Verify).
func (s *Signer) SignChunk(chunkUUID string, workerID int64, expires time.Time) (string, []byte) {
	h := sha256.Sum256([]byte(chunkUUID))
	return s.Sign(h[:], workerID, expires)
}

// VerifyChunk parses a chunk lease and asserts the embedded hash equals sha256(chunkUUID).
func (s *Signer) VerifyChunk(token, chunkUUID string) (workerID int64, expires time.Time, err error) {
	got, wid, exp, verr := s.Verify(token)
	if verr != nil {
		return 0, time.Time{}, verr
	}
	want := sha256.Sum256([]byte(chunkUUID))
	if !equalConstantTime(got, want[:]) {
		return 0, time.Time{}, errChunkMismatch
	}
	return wid, exp, nil
}

var errChunkMismatch = chunkErr("lease: chunk id mismatch")

type chunkErr string

func (e chunkErr) Error() string { return string(e) }

func equalConstantTime(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

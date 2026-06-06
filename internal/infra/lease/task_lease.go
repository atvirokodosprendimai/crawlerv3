package lease

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// SignTask issues a lease for an integer task ID. The 8-byte ID is sha256'd
// into the 32-byte hash slot of the standard lease layout.
func (s *Signer) SignTask(taskID int64, workerID int64, expires time.Time) (string, []byte) {
	h := taskHash(taskID)
	return s.Sign(h[:], workerID, expires)
}

// VerifyTask parses a task lease and asserts the embedded hash equals
// sha256("task:<id>").
func (s *Signer) VerifyTask(token string, taskID int64) (workerID int64, expires time.Time, err error) {
	got, wid, exp, verr := s.Verify(token)
	if verr != nil {
		return 0, time.Time{}, verr
	}
	want := taskHash(taskID)
	if !equalConstantTime(got, want[:]) {
		return 0, time.Time{}, errChunkMismatch // reuses opaque mismatch error
	}
	return wid, exp, nil
}

func taskHash(taskID int64) [32]byte {
	var buf [16]byte
	copy(buf[:], "task:")
	binary.BigEndian.PutUint64(buf[8:], uint64(taskID))
	return sha256.Sum256(buf[:])
}

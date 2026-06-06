// Package lease signs and verifies opaque lease tokens with HMAC-SHA256.
//
// Token format: base64url( urlHash[32] || workerID[8] || expiresUnix[8] || mac[16] )
// MAC = HMAC-SHA256(secret, urlHash || workerID || expiresUnix), truncated to 16 bytes.
package lease

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/base64"
	"errors"
	"time"
)

const (
	hashLen    = 32
	workerLen  = 8
	expiresLen = 8
	macLen     = 16
	totalLen   = hashLen + workerLen + expiresLen + macLen
)

// Signer issues and verifies lease tokens.
type Signer struct {
	secret []byte
}

// New returns a Signer with the given secret. secret must be >= 16 bytes.
func New(secret []byte) (*Signer, error) {
	if len(secret) < 16 {
		return nil, errors.New("lease: secret too short, need >=16 bytes")
	}
	return &Signer{secret: append([]byte(nil), secret...)}, nil
}

// Sign returns (token string, raw bytes) for a job lease.
func (s *Signer) Sign(urlHash []byte, workerID int64, expires time.Time) (string, []byte) {
	if len(urlHash) != hashLen {
		// Caller bug; refuse to silently truncate.
		panic("lease.Sign: urlHash must be 32 bytes")
	}
	buf := make([]byte, totalLen)
	copy(buf[:hashLen], urlHash)
	binary.BigEndian.PutUint64(buf[hashLen:hashLen+workerLen], uint64(workerID))
	binary.BigEndian.PutUint64(buf[hashLen+workerLen:hashLen+workerLen+expiresLen], uint64(expires.Unix()))
	mac := hmacOf(s.secret, buf[:hashLen+workerLen+expiresLen])
	copy(buf[hashLen+workerLen+expiresLen:], mac[:macLen])
	return base64.RawURLEncoding.EncodeToString(buf), buf
}

// Verify parses and validates a token. Returns urlHash, workerID, expiry.
func (s *Signer) Verify(token string) (urlHash []byte, workerID int64, expires time.Time, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, 0, time.Time{}, errors.New("lease: bad encoding")
	}
	if len(raw) != totalLen {
		return nil, 0, time.Time{}, errors.New("lease: bad length")
	}
	want := hmacOf(s.secret, raw[:hashLen+workerLen+expiresLen])
	if !hmac.Equal(want[:macLen], raw[hashLen+workerLen+expiresLen:]) {
		return nil, 0, time.Time{}, errors.New("lease: bad signature")
	}
	urlHash = raw[:hashLen]
	workerID = int64(binary.BigEndian.Uint64(raw[hashLen : hashLen+workerLen]))
	expires = time.Unix(int64(binary.BigEndian.Uint64(raw[hashLen+workerLen:hashLen+workerLen+expiresLen])), 0).UTC()
	if time.Now().After(expires) {
		return urlHash, workerID, expires, errors.New("lease: expired")
	}
	return urlHash, workerID, expires, nil
}

// Raw returns the raw bytes for a token, for storage / equality checks.
func Raw(token string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	if len(raw) != totalLen {
		return nil, errors.New("lease: bad length")
	}
	return raw, nil
}

func hmacOf(secret, data []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write(data)
	return h.Sum(nil)
}

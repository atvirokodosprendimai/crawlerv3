// Package local is a filesystem-backed BlobStore implementation.
package local

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// Store stores blobs under a root directory.
type Store struct {
	root string
}

// New creates the root directory if missing and returns a Store.
func New(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("local store: root required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("local store: mkdir root: %w", err)
	}
	return &Store{root: root}, nil
}

// Backend implements lake.BlobStore.
func (s *Store) Backend() string { return "local" }

func (s *Store) full(key string) string { return filepath.Join(s.root, filepath.FromSlash(key)) }

// Put streams r to disk and verifies the SHA matches the provided meta (if any).
func (s *Store) Put(ctx context.Context, key string, r io.Reader, m lake.PutMeta) (lake.Stat, error) {
	dst := s.full(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return lake.Stat{}, fmt.Errorf("local Put: mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".put-*")
	if err != nil {
		return lake.Stat{}, fmt.Errorf("local Put: tmp: %w", err)
	}
	tmpPath := tmp.Name()
	h := sha256.New()
	tee := io.TeeReader(r, h)
	n, copyErr := io.Copy(tmp, tee)
	if cerr := tmp.Close(); cerr != nil && copyErr == nil {
		copyErr = cerr
	}
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return lake.Stat{}, fmt.Errorf("local Put: copy: %w", copyErr)
	}
	got := h.Sum(nil)
	if len(m.SHA256) > 0 {
		if !equalBytes(got, m.SHA256) {
			_ = os.Remove(tmpPath)
			return lake.Stat{}, errors.New("local Put: sha256 mismatch")
		}
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		_ = os.Remove(tmpPath)
		return lake.Stat{}, fmt.Errorf("local Put: rename: %w", err)
	}
	return lake.Stat{
		Size:        n,
		ContentType: m.ContentType,
		SHA256:      got,
		ModTime:     time.Now().UTC(),
	}, nil
}

// Get opens the blob for reading.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, lake.Stat, error) {
	f, err := os.Open(s.full(key))
	if err != nil {
		return nil, lake.Stat{}, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, lake.Stat{}, err
	}
	return f, lake.Stat{Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// Stat returns metadata without opening the body.
func (s *Store) Stat(ctx context.Context, key string) (lake.Stat, error) {
	fi, err := os.Stat(s.full(key))
	if err != nil {
		return lake.Stat{}, err
	}
	return lake.Stat{Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// Delete removes the blob from disk.
func (s *Store) Delete(ctx context.Context, key string) error {
	return os.Remove(s.full(key))
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

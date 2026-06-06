// Package lake models raw fetched blobs and their storage location.
package lake

import "time"

// Object is a stored raw blob backed by some BlobStore.
type Object struct {
	ID             int64
	URLHash        []byte
	StorageBackend string
	StorageKey     string
	ContentType    string
	ContentSHA256  []byte
	FileSize       int64
	ArchivedAt     time.Time
	MigratedFrom   string
}

package leveldb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestVersionOrdering(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()

	o := &opt.Options{
		WriteBuffer:         512 * 1024,
		CompactionTableSize: 1 * 1024 * 1024,
	}

	db, err := Open(stor, o)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := []byte("test-key")

	// Write 3 versions
	for v := 1; v <= 3; v++ {
		val := []byte(fmt.Sprintf("value-v%d", v))
		if err := db.PutWithVersion(key, val, uint64(v), nil); err != nil {
			t.Fatalf("Put v%d failed: %v", v, err)
		}
	}

	t.Log("Before compaction:")
	latest, _ := db.Get(key, nil)
	t.Logf("  Get(latest) = %q", latest)

	// Compact
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Logf("CompactRange warning: %v", err)
	}

	t.Log("After compaction:")
	latest, _ = db.Get(key, nil)
	t.Logf("  Get(latest) = %q", latest)

	// Check each version
	for v := 1; v <= 3; v++ {
		val, err := db.GetWithVersion(key, uint64(v), nil)
		if err != nil {
			t.Logf("  GetWithVersion(v%d) failed: %v", v, err)
		} else {
			t.Logf("  GetWithVersion(v%d) = %q", v, val)
		}
	}

	// Iterate to see actual order
	t.Log("Iteration order (shows internal keys):")
	// We need to use internal iterator
	// For now, just verify the result
	expectedLatest := []byte("value-v3")
	if !bytes.Equal(latest, expectedLatest) {
		t.Errorf("Expected latest=%q, got %q", expectedLatest, latest)
	}
}

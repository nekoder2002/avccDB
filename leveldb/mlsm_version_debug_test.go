package leveldb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// TestVersionQueryDebug debugs version query issue
func TestVersionQueryDebug(t *testing.T) {
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

	// Write versions 1, 2, 3 for key "large-key-00010"
	key := []byte("large-key-00010")
	for v := 1; v <= 3; v++ {
		val := []byte(fmt.Sprintf("large-val-00010-v%02d", v))
		if err := db.PutWithVersion(key, val, uint64(v), nil); err != nil {
			t.Fatalf("Put v%d failed: %v", v, err)
		}
		t.Logf("Wrote version %d: %q", v, val)
	}

	// Test before compaction
	t.Log("Before compaction:")
	for v := 1; v <= 3; v++ {
		val, err := db.GetWithVersion(key, uint64(v), nil)
		if err != nil {
			t.Errorf("  GetWithVersion(%d) failed: %v", v, err)
		} else {
			t.Logf("  GetWithVersion(%d) = %q", v, val)
		}
	}
	latest, err := db.Get(key, nil)
	if err != nil {
		t.Errorf("  Get(latest) failed: %v", err)
	} else {
		t.Logf("  Get(latest) = %q", latest)
	}

	// Compact
	t.Log("Compacting...")
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Logf("CompactRange warning: %v", err)
	}

	// Test after compaction
	t.Log("After compaction:")
	for v := 1; v <= 3; v++ {
		val, err := db.GetWithVersion(key, uint64(v), nil)
		if err != nil {
			t.Errorf("  GetWithVersion(%d) failed: %v", v, err)
		} else {
			t.Logf("  GetWithVersion(%d) = %q", v, val)
			expectedVal := []byte(fmt.Sprintf("large-val-00010-v%02d", v))
			if !bytes.Equal(val, expectedVal) {
				t.Errorf("  Version %d mismatch: got %q want %q", v, val, expectedVal)
			}
		}
	}

	latest, err = db.Get(key, nil)
	if err != nil {
		t.Fatalf("Get(latest) failed: %v", err)
	}
	t.Logf("  Get(latest) = %q", latest)

	expectedLatest := []byte("large-val-00010-v03")
	if !bytes.Equal(latest, expectedLatest) {
		t.Fatalf("Latest mismatch: got %q want %q", latest, expectedLatest)
	}

	t.Log("✓ All version queries correct!")
}

package leveldb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// TestBatchVersionDebug debugs batch version query issue
func TestBatchVersionDebug(t *testing.T) {
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

	// Write keys 0-20, each with 3 versions
	t.Log("Writing keys 0-20 with 3 versions each...")
	for i := 0; i < 20; i++ {
		for v := 1; v <= 3; v++ {
			key := []byte(fmt.Sprintf("large-key-%05d", i))
			val := []byte(fmt.Sprintf("large-val-%05d-v%02d", i, v))
			if err := db.PutWithVersion(key, val, uint64(v), nil); err != nil {
				t.Fatalf("Put key=%d v=%d failed: %v", i, v, err)
			}
		}
	}

	// Test before compaction
	t.Log("Before compaction - testing keys 0, 5, 10, 15:")
	for _, i := range []int{0, 5, 10, 15} {
		key := []byte(fmt.Sprintf("large-key-%05d", i))

		// Test each version
		for v := 1; v <= 3; v++ {
			val, err := db.GetWithVersion(key, uint64(v), nil)
			expectedVal := []byte(fmt.Sprintf("large-val-%05d-v%02d", i, v))
			if err != nil {
				t.Errorf("  Key %d v%d: GetWithVersion failed: %v", i, v, err)
			} else if !bytes.Equal(val, expectedVal) {
				t.Errorf("  Key %d v%d: mismatch got %q want %q", i, v, val, expectedVal)
			}
		}

		// Test latest
		latest, err := db.Get(key, nil)
		expectedLatest := []byte(fmt.Sprintf("large-val-%05d-v03", i))
		if err != nil {
			t.Errorf("  Key %d: Get(latest) failed: %v", i, err)
		} else if !bytes.Equal(latest, expectedLatest) {
			t.Errorf("  Key %d: Get(latest) mismatch got %q want %q", i, latest, expectedLatest)
		} else {
			t.Logf("  Key %d: ✓ All versions correct", i)
		}
	}

	// Compact
	t.Log("Compacting...")
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Logf("CompactRange warning: %v", err)
	}

	// Test after compaction
	t.Log("After compaction - testing keys 0, 5, 10, 15:")
	failedKeys := []int{}
	for _, i := range []int{0, 5, 10, 15} {
		key := []byte(fmt.Sprintf("large-key-%05d", i))

		// Debug: check GetWithVersion for each version first
		t.Logf("  Key %d version tests:", i)
		for v := 1; v <= 3; v++ {
			val, err := db.GetWithVersion(key, uint64(v), nil)
			if err != nil {
				t.Logf("    v%d: ERROR %v", v, err)
			} else {
				t.Logf("    v%d: %q", v, val)
			}
		}

		// Now test latest
		latest, err := db.Get(key, nil)
		if err != nil {
			t.Logf("    latest: ERROR %v", err)
		} else {
			t.Logf("    latest: %q", latest)
		}

		// Validate
		allVersionsOk := true
		for v := 1; v <= 3; v++ {
			val, err := db.GetWithVersion(key, uint64(v), nil)
			expectedVal := []byte(fmt.Sprintf("large-val-%05d-v%02d", i, v))
			if err != nil {
				t.Errorf("  Key %d v%d: GetWithVersion failed: %v", i, v, err)
				allVersionsOk = false
			} else if !bytes.Equal(val, expectedVal) {
				t.Errorf("  Key %d v%d: mismatch got %q want %q", i, v, val, expectedVal)
				allVersionsOk = false
			}
		}

		// Test latest
		expectedLatest := []byte(fmt.Sprintf("large-val-%05d-v03", i))
		if err != nil {
			t.Errorf("  Key %d: Get(latest) failed: %v", i, err)
			failedKeys = append(failedKeys, i)
		} else if !bytes.Equal(latest, expectedLatest) {
			t.Errorf("  Key %d: Get(latest) mismatch got %q want %q", i, latest, expectedLatest)
			failedKeys = append(failedKeys, i)
		} else if allVersionsOk {
			t.Logf("  Key %d: ✓ All versions correct", i)
		}
	}

	if len(failedKeys) > 0 {
		t.Errorf("Failed keys after compaction: %v", failedKeys)

		// Debug: use iterator to check what's actually stored
		t.Log("Debug: Checking stored keys via iterator...")
		iter := db.NewIterator(nil, nil)
		defer iter.Release()

		count := 0
		for iter.Next() {
			if count < 10 {
				t.Logf("  Stored: key=%x val=%q", iter.Key(), iter.Value())
			}
			count++
		}
		t.Logf("  Total keys in DB: %d", count)
	}
}

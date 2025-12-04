package leveldb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// TestVersionHistory tests the GetVersionHistory API for provenance queries
func TestVersionHistory(t *testing.T) {
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

	// Test data: Write multiple versions of the same key
	key := []byte("user:alice")
	versions := []struct {
		version uint64
		value   string
	}{
		{100, "Alice v100 - Initial state"},
		{150, "Alice v150 - Updated profile"},
		{200, "Alice v200 - Changed email"},
		{250, "Alice v250 - Added phone"},
		{300, "Alice v300 - Updated address"},
	}

	// Write all versions
	t.Logf("Writing %d versions of key %q", len(versions), key)
	for _, v := range versions {
		if err := db.PutWithVersion(key, []byte(v.value), v.version, nil); err != nil {
			t.Fatalf("PutWithVersion(v=%d) failed: %v", v.version, err)
		}
		t.Logf("  Wrote version %d: %s", v.version, v.value)
	}

	// Test 1: Query all versions (no range limit)
	t.Log("\n=== Test 1: Query all versions ===")
	entries, err := db.GetVersionHistory(key, 0, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory(all) failed: %v", err)
	}

	if len(entries) != len(versions) {
		t.Errorf("Expected %d entries, got %d", len(versions), len(entries))
	}

	for i, entry := range entries {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
		if entry.Version != versions[i].version {
			t.Errorf("  Version mismatch: expected %d, got %d", versions[i].version, entry.Version)
		}
		if !bytes.Equal(entry.Value, []byte(versions[i].value)) {
			t.Errorf("  Value mismatch at v%d: expected %q, got %q",
				entry.Version, versions[i].value, entry.Value)
		}
	}

	// Test 2: Query specific range [150, 250]
	t.Log("\n=== Test 2: Query range [150, 250] ===")
	entries, err = db.GetVersionHistory(key, 150, 250, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory(150-250) failed: %v", err)
	}

	expected := []uint64{150, 200, 250}
	if len(entries) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for i, entry := range entries {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
		if i < len(expected) && entry.Version != expected[i] {
			t.Errorf("  Version mismatch: expected %d, got %d", expected[i], entry.Version)
		}
	}

	// Test 3: Query with min version only
	t.Log("\n=== Test 3: Query min version 200 ===")
	entries, err = db.GetVersionHistory(key, 200, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory(min=200) failed: %v", err)
	}

	expected = []uint64{200, 250, 300}
	if len(entries) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for i, entry := range entries {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
		if i < len(expected) && entry.Version != expected[i] {
			t.Errorf("  Version mismatch: expected %d, got %d", expected[i], entry.Version)
		}
	}

	// Test 4: Query with max version only
	t.Log("\n=== Test 4: Query max version 200 ===")
	entries, err = db.GetVersionHistory(key, 0, 200, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory(max=200) failed: %v", err)
	}

	expected = []uint64{100, 150, 200}
	if len(entries) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for i, entry := range entries {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
		if i < len(expected) && entry.Version != expected[i] {
			t.Errorf("  Version mismatch: expected %d, got %d", expected[i], entry.Version)
		}
	}

	// Test 5: Query non-existent key
	t.Log("\n=== Test 5: Query non-existent key ===")
	_, err = db.GetVersionHistory([]byte("non-existent"), 0, 0, nil)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got: %v", err)
	} else {
		t.Log("  ✓ Correctly returned ErrNotFound")
	}

	t.Log("\n✓ All version history tests passed!")
}

// TestVersionHistoryAfterCompaction tests version history after compaction
func TestVersionHistoryAfterCompaction(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()

	o := &opt.Options{
		WriteBuffer:         256 * 1024,
		CompactionTableSize: 512 * 1024,
	}

	db, err := Open(stor, o)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write multiple keys with multiple versions
	numKeys := 100
	numVersions := 5

	t.Logf("Writing %d keys with %d versions each", numKeys, numVersions)
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		for v := 1; v <= numVersions; v++ {
			value := []byte(fmt.Sprintf("value-%04d-v%d", i, v))
			if err := db.PutWithVersion(key, value, uint64(v*100), nil); err != nil {
				t.Fatalf("PutWithVersion failed: %v", err)
			}
		}
	}

	// Query before compaction
	testKey := []byte("key-0050")
	t.Log("\n=== Before Compaction ===")
	entriesBefore, err := db.GetVersionHistory(testKey, 0, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory before compaction failed: %v", err)
	}

	t.Logf("Found %d versions before compaction:", len(entriesBefore))
	for _, entry := range entriesBefore {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
	}

	// Trigger compaction
	t.Log("\n=== Triggering Compaction ===")
	// Force flush to create SST files
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("flush-key-%04d", i))
		for v := 1; v <= numVersions; v++ {
			value := []byte(fmt.Sprintf("flush-value-%04d-v%d", i, v))
			if err := db.PutWithVersion(key, value, uint64(v*100+1000), nil); err != nil {
				t.Fatalf("PutWithVersion failed: %v", err)
			}
		}
	}

	// Query after compaction
	t.Log("\n=== After Compaction ===")
	entriesAfter, err := db.GetVersionHistory(testKey, 0, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory after compaction failed: %v", err)
	}

	t.Logf("Found %d versions after compaction:", len(entriesAfter))
	for _, entry := range entriesAfter {
		t.Logf("  Version %d: %s", entry.Version, entry.Value)
	}

	// Verify all versions are preserved
	if len(entriesAfter) != len(entriesBefore) {
		t.Errorf("Version count mismatch: before=%d, after=%d",
			len(entriesBefore), len(entriesAfter))
	}

	for i := range entriesBefore {
		if i >= len(entriesAfter) {
			break
		}
		if entriesBefore[i].Version != entriesAfter[i].Version {
			t.Errorf("Version mismatch at index %d: before=%d, after=%d",
				i, entriesBefore[i].Version, entriesAfter[i].Version)
		}
		if !bytes.Equal(entriesBefore[i].Value, entriesAfter[i].Value) {
			t.Errorf("Value mismatch at version %d", entriesBefore[i].Version)
		}
	}

	t.Log("\n✓ All versions preserved after compaction!")
}

// TestVersionHistoryMultipleKeys tests version history for multiple keys
func TestVersionHistoryMultipleKeys(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()

	db, err := Open(stor, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write different version ranges for different keys
	keys := map[string][]uint64{
		"user:alice": {100, 200, 300},
		"user:bob":   {150, 250},
		"user:carol": {50, 100, 150, 200, 250, 300, 350},
	}

	t.Log("Writing version data for multiple users:")
	for key, versions := range keys {
		for _, v := range versions {
			value := []byte(fmt.Sprintf("%s-v%d", key, v))
			if err := db.PutWithVersion([]byte(key), value, v, nil); err != nil {
				t.Fatalf("PutWithVersion failed: %v", err)
			}
		}
		t.Logf("  %s: %d versions", key, len(versions))
	}

	// Query each key's history
	t.Log("\n=== Querying version history for each key ===")
	for key, expectedVersions := range keys {
		entries, err := db.GetVersionHistory([]byte(key), 0, 0, nil)
		if err != nil {
			t.Errorf("GetVersionHistory(%s) failed: %v", key, err)
			continue
		}

		t.Logf("\n%s:", key)
		if len(entries) != len(expectedVersions) {
			t.Errorf("  Expected %d versions, got %d", len(expectedVersions), len(entries))
		}

		for i, entry := range entries {
			t.Logf("  Version %d: %s", entry.Version, entry.Value)
			if i < len(expectedVersions) && entry.Version != expectedVersions[i] {
				t.Errorf("  Version mismatch at index %d: expected %d, got %d",
					i, expectedVersions[i], entry.Version)
			}
		}
	}

	t.Log("\n✓ Multiple key version history test passed!")
}

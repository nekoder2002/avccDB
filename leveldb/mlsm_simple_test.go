package leveldb

import (
	"bytes"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestSimpleGetLatest(t *testing.T) {
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
	value := []byte("test-value-v3")

	// Write only version 3
	if err := db.PutWithVersion(key, value, 3, nil); err != nil {
		t.Fatalf("PutWithVersion failed: %v", err)
	}

	// Try Get (latest)
	gotLatest, err := db.Get(key, nil)
	if err != nil {
		t.Errorf("Get(latest) failed: %v", err)
	} else if !bytes.Equal(gotLatest, value) {
		t.Errorf("Get(latest) mismatch: got %q, want %q", gotLatest, value)
	} else {
		t.Logf("✓ Get(latest) before compaction: %q", gotLatest)
	}

	// Try GetWithVersion(3)
	gotV3, err := db.GetWithVersion(key, 3, nil)
	if err != nil {
		t.Fatalf("GetWithVersion(3) failed: %v", err)
	} else if !bytes.Equal(gotV3, value) {
		t.Errorf("GetWithVersion(3) mismatch: got %q, want %q", gotV3, value)
	} else {
		t.Logf("✓ GetWithVersion(3) before compaction: %q", gotV3)
	}

	// Now compact
	t.Log("Triggering compaction...")
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Logf("CompactRange warning: %v", err)
	}

	// Try GetWithVersion(3) after compaction
	t.Log("Testing GetWithVersion(3) after compaction...")
	var gotV3After []byte
	gotV3After, err = db.GetWithVersion(key, 3, nil)
	if err != nil {
		t.Fatalf("GetWithVersion(3) after compaction failed: %v", err)
	} else if !bytes.Equal(gotV3After, value) {
		t.Fatalf("GetWithVersion(3) value mismatch after compaction: got %q, want %q", gotV3After, value)
	}
	t.Logf("✓ GetWithVersion(3) after compaction: %q", gotV3After)

	// Try Get (latest) after compaction
	t.Log("Testing Get(latest) after compaction...")
	t.Logf("Query key: %x (len=%d)", key, len(key))
	gotLatestAfter, err := db.Get(key, nil)
	if err != nil {
		t.Errorf("Get(latest) after compaction failed: %v", err)

		// Debug: try to iterate and see what's there
		iter := db.NewIterator(nil, nil)
		defer iter.Release()
		found := false
		for iter.Next() {
			iterKey := iter.Key()
			iterVal := iter.Value()
			t.Logf("Iterator found: key=%x (len=%d), value=%q", iterKey, len(iterKey), iterVal)
			t.Logf("  Search key: key=%x (len=%d)", key, len(key))
			// Iterator returns user keys, not internal keys
			if bytes.Equal(iterKey, key) {
				t.Logf("  -> Match! User key equals search key")
				found = true
				break
			} else {
				t.Logf("  -> No match: %x != %x", iterKey, key)
			}
		}
		if !found {
			t.Fatalf("Iterator didn't find the key either! This means data was lost")
		} else {
			t.Fatalf("Iterator found the key but Get() failed - this is the lookup bug")
		}
	} else if !bytes.Equal(gotLatestAfter, value) {
		t.Errorf("Get(latest) after compaction mismatch: got %q, want %q", gotLatestAfter, value)
	} else {
		t.Logf("✓ Get(latest) after compaction: %q", gotLatestAfter)
	}
}

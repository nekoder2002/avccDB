package leveldb

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

func TestComparerWithKeyMaxSeq(t *testing.T) {
	icmp := &iComparer{ucmp: comparer.DefaultComparer}

	// Create a stored key: test-key|version=3|seq=100
	storedKey := makeInternalKeyWithVersion(nil, []byte("test-key"), 3, 100, keyTypeVal)
	t.Logf("Stored key: %x (len=%d)", storedKey, len(storedKey))

	// Create a query key: test-key|version=keyMaxSeq|seq=keyMaxSeq
	queryKey := makeInternalKeyWithVersion(nil, []byte("test-key"), keyMaxSeq, keyMaxSeq, keyTypeSeek)
	t.Logf("Query key:  %x (len=%d)", queryKey, len(queryKey))

	// Compare them
	cmp := icmp.Compare(storedKey, queryKey)
	t.Logf("Compare(stored, query) = %d", cmp)
	t.Logf("Expected: 1 (stored > query) because keyMaxSeq sorts before actual versions")

	if cmp != 1 {
		t.Errorf("Comparison should return 1 (stored > query), got %d", cmp)
	}

	// Also test the reverse - query key should sort before stored key
	cmpRev := icmp.Compare(queryKey, storedKey)
	t.Logf("Compare(query, stored) = %d", cmpRev)
	t.Logf("Expected: -1 (query < stored) for Seek to find the first actual version")

	if cmpRev != -1 {
		t.Errorf("Reverse comparison should return -1 (query < stored), got %d", cmpRev)
	}

	// Test that this ensures Seek behavior finds the highest sequence
	// Create two stored keys with different sequences
	storedKey1 := makeInternalKeyWithVersion(nil, []byte("test-key"), 3, 100, keyTypeVal)
	storedKey2 := makeInternalKeyWithVersion(nil, []byte("test-key"), 3, 200, keyTypeVal)

	// Higher seq should sort first (descending)
	cmpSeq := icmp.Compare(storedKey1, storedKey2)
	t.Logf("Compare(seq=100, seq=200) = %d", cmpSeq)
	if cmpSeq != 1 {
		t.Errorf("Lower seq should sort after higher seq, got %d", cmpSeq)
	}
}

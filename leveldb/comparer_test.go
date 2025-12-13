package leveldb

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb/dbkey"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

func TestComparerWithKeyMaxSeq(t *testing.T) {
	icmp := &iComparer{ucmp: comparer.DefaultComparer}

	// Create a stored key: test-key|version=3|seq=100
	storedKey := dbkey.MakeInternalKeyWithVersion(nil, []byte("test-key"), 3, 100, dbkey.KeyTypeVal)
	t.Logf("Stored key: %x (len=%d)", storedKey, len(storedKey))

	// Create a query key: test-key|version=KeyMaxSeq|seq=KeyMaxSeq
	queryKey := dbkey.MakeInternalKeyWithVersion(nil, []byte("test-key"), dbkey.LastestVersion, dbkey.KeyMaxSeq, dbkey.KeyTypeSeek)
	t.Logf("Query key:  %x (len=%d)", queryKey, len(queryKey))

	// Compare them
	cmp := icmp.Compare(storedKey, queryKey)
	t.Logf("Compare(stored, query) = %d", cmp)
	t.Logf("Expected: 1 (stored > query) because KeyMaxSeq sorts before actual versions")

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
	storedKey1 := dbkey.MakeInternalKeyWithVersion(nil, []byte("test-key"), 3, 100, dbkey.KeyTypeVal)
	storedkey := dbkey.MakeInternalKeyWithVersion(nil, []byte("test-key"), 3, 200, dbkey.KeyTypeVal)

	// Higher seq should sort first (descending)
	cmpSeq := icmp.Compare(storedKey1, storedkey)
	t.Logf("Compare(seq=100, seq=200) = %d", cmpSeq)
	if cmpSeq != 1 {
		t.Errorf("Lower seq should sort after higher seq, got %d", cmpSeq)
	}
}

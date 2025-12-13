package leveldb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

// TestProvenanceWithProof tests GetVersionHistoryWithProof functionality
func TestProvenanceWithProof(t *testing.T) {
	dbPath := "testdata/provenance_test"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	// Open database
	db, err := OpenFile(dbPath, &opt.Options{
		WriteBuffer:            1 * 1024 * 1024, // 1MB - smaller to trigger flush
		CompactionTableSize:    2 * 1024 * 1024, // 2MB
		DisableSeeksCompaction: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Logf("\n=== 溯源查询带证明测试 ===\n")

	// Test key
	key := []byte("test-key")

	// Write multiple versions with larger data to trigger flush
	numVersions := 10
	dataSize := 200 * 1024 // 200KB per version to trigger flush
	t.Logf("[阶段1] 写入 %d 个版本...", numVersions)
	for v := 1; v <= numVersions; v++ {
		value := make([]byte, dataSize)
		copy(value, []byte(fmt.Sprintf("value-v%02d-data-", v)))
		err := db.PutWithVersion(key, value, uint64(v), nil)
		if err != nil {
			t.Fatalf("Failed to write version %d: %v", v, err)
		}
	}
	t.Logf("✓ 写入完成\n")

	// Query all versions without proof
	t.Logf("[阶段2] 查询所有版本（不带证明）...")
	entries, err := db.GetVersionHistory(key, 0, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistory failed: %v", err)
	}

	if len(entries) != numVersions {
		t.Errorf("Expected %d versions, got %d", numVersions, len(entries))
	}

	for i, entry := range entries {
		expectedVersion := uint64(i + 1)
		expectedPrefix := []byte(fmt.Sprintf("value-v%02d-data-", expectedVersion))

		if entry.Version != expectedVersion {
			t.Errorf("Entry %d: expected version %d, got %d", i, expectedVersion, entry.Version)
		}
		if !bytes.HasPrefix(entry.Value, expectedPrefix) {
			t.Errorf("Entry %d: value mismatch", i)
		}
		if entry.Proof != nil {
			t.Errorf("Entry %d: proof should be nil when not requested", i)
		}
	}
	t.Logf("✓ 查询到 %d 个版本，无证明\n", len(entries))

	// Query all versions WITH proof
	t.Logf("[阶段3] 查询所有版本（带证明）...")
	entriesWithProof, err := db.GetVersionHistoryWithProof(key, 0, 0, nil)
	if err != nil {
		t.Fatalf("GetVersionHistoryWithProof failed: %v", err)
	}

	if len(entriesWithProof) != numVersions {
		t.Errorf("Expected %d versions, got %d", numVersions, len(entriesWithProof))
	}

	proofCount := 0
	verifiedCount := 0

	for i, entry := range entriesWithProof {
		expectedVersion := uint64(i + 1)
		expectedPrefix := []byte(fmt.Sprintf("value-v%02d-data-", expectedVersion))

		if entry.Version != expectedVersion {
			t.Errorf("Entry %d: expected version %d, got %d", i, expectedVersion, entry.Version)
		}
		if !bytes.HasPrefix(entry.Value, expectedPrefix) {
			t.Errorf("Entry %d: value mismatch", i)
		}

		// Check proof
		if entry.Proof != nil {
			proofCount++
			// Verify proof
			if entry.Proof.Verify(key, entry.Version, entry.Value) {
				verifiedCount++
			} else {
				t.Errorf("Entry %d: proof verification failed for version %d", i, entry.Version)
			}
		}
	}

	t.Logf("✓ 查询到 %d 个版本，%d 个证明，%d 个验证通过\n", len(entriesWithProof), proofCount, verifiedCount)

	// Query version range with proof
	t.Logf("[阶段4] 查询版本范围 [3, 7]（带证明）...")
	rangeEntries, err := db.GetVersionHistoryWithProof(key, 3, 7, nil)
	if err != nil {
		t.Fatalf("GetVersionHistoryWithProof (range) failed: %v", err)
	}

	expectedCount := 5 // versions 3, 4, 5, 6, 7
	if len(rangeEntries) != expectedCount {
		t.Errorf("Expected %d versions in range [3,7], got %d", expectedCount, len(rangeEntries))
	}

	rangeProofCount := 0
	rangeVerifiedCount := 0

	for i, entry := range rangeEntries {
		expectedVersion := uint64(3 + i)

		if entry.Version != expectedVersion {
			t.Errorf("Range entry %d: expected version %d, got %d", i, expectedVersion, entry.Version)
		}

		// Check proof
		if entry.Proof != nil {
			rangeProofCount++
			if entry.Proof.Verify(key, entry.Version, entry.Value) {
				rangeVerifiedCount++
			} else {
				t.Errorf("Range entry %d: proof verification failed for version %d", i, entry.Version)
			}
		}
	}

	t.Logf("✓ 范围查询到 %d 个版本，%d 个证明，%d 个验证通过\n", len(rangeEntries), rangeProofCount, rangeVerifiedCount)

	// Summary
	t.Logf("\n=== 测试总结 ===")
	t.Logf("无证明查询: %d 个版本", len(entries))
	t.Logf("带证明查询: %d 个版本, %d 个证明验证通过", len(entriesWithProof), verifiedCount)
	t.Logf("范围查询: %d 个版本, %d 个证明验证通过", len(rangeEntries), rangeVerifiedCount)

	if verifiedCount > 0 && rangeVerifiedCount > 0 {
		t.Logf("\n✅ 所有溯源查询带证明测试通过！")
	}
}

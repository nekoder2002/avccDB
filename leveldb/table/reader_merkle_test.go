// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package table

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestTableReaderMerkle(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test_merkle_*.sst")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create test data
	testData := []struct {
		key     string
		value   string
		version uint64
	}{
		{"key001", "value001", 1},
		{"key002", "value002", 1},
		{"key003", "value003", 1},
		{"key004", "value004", 2},
		{"key005", "value005", 2},
	}

	// Write SST file with Merkle tree
	o := &opt.Options{
		BlockRestartInterval: 1,
		BlockSize:            4 * 1024,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
	}

	bpool := util.NewBufferPool(4096)
	writer := NewWriter(tmpFile, o, bpool, 0) // size=0 for auto

	// Enable Merkle tree
	writer.enableMerkle = true

	for _, td := range testData {
		// Create versioned key: ukey | version | seq+type
		key := makeVersionedKey([]byte(td.key), td.version, 1)
		value := []byte(td.value)
		if err := writer.Append(key, value); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Get file size
	stat, err := tmpFile.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Open reader
	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	reader, err := NewReader(tmpFile, stat.Size(), fd, nil, bpool, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Release()

	// Verify Merkle tree is enabled
	if !reader.merkleEnabled {
		t.Fatal("Merkle tree should be enabled")
	}

	// Test GetWithProof
	ro := &opt.ReadOptions{}
	testKey := makeVersionedKey([]byte("key003"), 1, 1)
	value, proof, err := reader.GetWithProof(testKey, ro)
	if err != nil {
		t.Fatalf("GetWithProof failed: %v", err)
	}

	if value == nil {
		t.Fatal("Value should not be nil")
	}

	if !bytes.Equal(value, []byte("value003")) {
		t.Fatalf("Value mismatch, got %s, want value003", string(value))
	}

	if proof == nil {
		t.Fatal("Proof should not be nil")
	}

	t.Logf("✓ GetWithProof successful")
	t.Logf("  - Key: %s", "key003")
	t.Logf("  - Value: %s", string(value))
	t.Logf("  - Root Hash: %x", proof.Root[:8])
}

func TestTableReaderWithoutMerkle(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test_no_merkle_*.sst")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write SST file without Merkle tree
	o := &opt.Options{
		BlockRestartInterval: 1,
		BlockSize:            4 * 1024,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
	}

	bpool := util.NewBufferPool(4096)
	writer := NewWriter(tmpFile, o, bpool, 0) // size=0 for auto

	// Don't enable Merkle tree
	writer.enableMerkle = false

	key := []byte("testkey")
	value := []byte("testvalue")
	if err := writer.Append(key, value); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Get file size
	stat, err := tmpFile.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Open reader
	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	reader, err := NewReader(tmpFile, stat.Size(), fd, nil, bpool, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Release()

	// Verify Merkle tree is not enabled
	if reader.merkleEnabled {
		t.Fatal("Merkle tree should not be enabled")
	}

	// Test GetWithProof (should return value but no proof)
	ro := &opt.ReadOptions{}
	val, proof, err := reader.GetWithProof(key, ro)
	if err != nil {
		t.Fatalf("GetWithProof failed: %v", err)
	}

	if !bytes.Equal(val, value) {
		t.Fatalf("Value mismatch, got %s, want %s", string(val), string(value))
	}

	if proof != nil {
		t.Fatal("Proof should be nil for table without Merkle tree")
	}

	t.Logf("✓ Non-Merkle table works correctly")
}

// Helper function to create versioned key
// Format: ukey | version (8 bytes) | seq+type (8 bytes)
func makeVersionedKey(ukey []byte, version, seq uint64) []byte {
	key := make([]byte, len(ukey)+16)
	copy(key, ukey)
	binary.LittleEndian.PutUint64(key[len(ukey):], version)
	binary.LittleEndian.PutUint64(key[len(ukey)+8:], (seq<<8)|uint64(1)) // type=1 (value)
	return key
}

func TestTableReaderLoadMerkleTree(t *testing.T) {
	// Create a temporary file with Merkle tree
	tmpFile, err := os.CreateTemp("", "test_load_merkle_*.sst")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write SST with small dataset
	o := &opt.Options{
		BlockRestartInterval: 1,
		BlockSize:            4 * 1024,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
		Filter:               filter.NewBloomFilter(10),
	}

	bpool := util.NewBufferPool(4096)
	writer := NewWriter(tmpFile, o, bpool, 0) // size=0 for auto
	writer.enableMerkle = true

	// Write 10 keys
	for i := 0; i < 10; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i)
		}
		if err := writer.Append(key, value); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Log writer stats
	t.Logf("Writer Stats:")
	t.Logf("  - Merkle enabled: %v", writer.enableMerkle)
	t.Logf("  - Block hashes collected: %d", len(writer.blockHashes))
	if writer.merkleTree != nil {
		t.Logf("  - Merkle tree root: %x", writer.merkleTree.Hash[:16])
	}

	// Reopen and load Merkle tree
	stat, err := tmpFile.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	reader, err := NewReader(tmpFile, stat.Size(), fd, nil, bpool, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Release()

	// Check if Merkle tree is enabled
	if !reader.merkleEnabled {
		t.Fatal("Merkle tree should be enabled after reading meta block")
	}

	// Load Merkle tree
	if err := reader.loadMerkleTree(); err != nil {
		t.Fatalf("Failed to load Merkle tree: %v", err)
	}

	if reader.merkleTree == nil {
		t.Fatal("Merkle tree should be loaded")
	}

	rootHash := reader.merkleTree.GetRoot()
	if rootHash.IsZero() {
		t.Fatal("Root hash should not be zero")
	}

	t.Logf("✓ Merkle tree loaded successfully")
	t.Logf("  - Root Hash: %x", rootHash[:16])
}

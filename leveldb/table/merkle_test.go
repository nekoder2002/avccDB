// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package table

import (
	"bytes"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/merkle"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// TestMerkleWriteAndRead tests that Merkle tree is properly written and can be read back
func TestMerkleWriteAndRead(t *testing.T) {
	// Create test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	// Create in-memory storage
	stor := storage.NewMemStorage()
	defer stor.Close()

	// Write table with Merkle tree
	tw, err := stor.Create(storage.FileDesc{Type: storage.TypeTable, Num: 1})
	if err != nil {
		t.Fatalf("Failed to create table writer: %v", err)
	}

	o := &opt.Options{
		BlockSize:            4096,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
		BlockRestartInterval: 16,
	}

	w := NewWriter(tw, o, nil, 4096)

	// Write key-value pairs
	for _, kv := range testData {
		if err := w.Append([]byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("Failed to append key-value: %v", err)
		}
	}

	// Close writer to finalize Merkle tree
	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
	tw.Close()

	// Read table back
	tr, err := stor.Open(storage.FileDesc{Type: storage.TypeTable, Num: 1})
	if err != nil {
		t.Fatalf("Failed to open table reader: %v", err)
	}
	defer tr.Close()

	// Get file size
	size, err := tr.Seek(0, 2) // Seek to end to get size
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	tr.Seek(0, 0) // Seek back to start

	r, err := NewReader(tr, size, storage.FileDesc{Type: storage.TypeTable, Num: 1}, nil, nil, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer r.Release()

	// Verify all key-value pairs can be read
	for _, kv := range testData {
		key := []byte(kv.key)
		value, err := r.Get(key, nil)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", kv.key, err)
			continue
		}
		if !bytes.Equal(value, []byte(kv.value)) {
			t.Errorf("Value mismatch for key %s: got %s, want %s", kv.key, value, kv.value)
		}
	}

	// Get Merkle root
	rootHash, err := r.GetMerkleRoot()
	if err != nil {
		t.Fatalf("Failed to get Merkle root: %v", err)
	}

	if rootHash.IsZero() {
		t.Error("Merkle root hash is zero")
	}

	t.Logf("Merkle root hash: %s", rootHash.String())
}

// TestMerkleProofGeneration tests Merkle proof generation and verification
func TestMerkleProofGeneration(t *testing.T) {
	// Create test data
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "red"},
		{"date", "brown"},
		{"elderberry", "purple"},
		{"fig", "purple"},
		{"grape", "green"},
	}

	// Create in-memory storage
	stor := storage.NewMemStorage()
	defer stor.Close()

	// Write table with Merkle tree
	tw, err := stor.Create(storage.FileDesc{Type: storage.TypeTable, Num: 2})
	if err != nil {
		t.Fatalf("Failed to create table writer: %v", err)
	}

	o := &opt.Options{
		BlockSize:            4096,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
		BlockRestartInterval: 16,
	}

	w := NewWriter(tw, o, nil, 4096)

	// Write key-value pairs
	for _, kv := range testData {
		if err := w.Append([]byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("Failed to append key-value: %v", err)
		}
	}

	// Close writer
	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
	tw.Close()

	// Read table back
	tr, err := stor.Open(storage.FileDesc{Type: storage.TypeTable, Num: 2})
	if err != nil {
		t.Fatalf("Failed to open table reader: %v", err)
	}
	defer tr.Close()

	size, err := tr.Seek(0, 2)
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	tr.Seek(0, 0)

	r, err := NewReader(tr, size, storage.FileDesc{Type: storage.TypeTable, Num: 2}, nil, nil, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer r.Release()

	// Get Merkle root
	rootHash, err := r.GetMerkleRoot()
	if err != nil {
		t.Fatalf("Failed to get Merkle root: %v", err)
	}

	t.Logf("Merkle root: %s", rootHash.String())

	// Test proof generation and verification for each key-value pair
	for i, kv := range testData {
		key := []byte(kv.key)
		expectedValue := []byte(kv.value)

		// Get the value and proof together
		_, gotValue, proof, err := r.GetWithProof(key, nil)
		if err != nil {
			t.Errorf("Failed to get value and proof for key %s: %v", kv.key, err)
			continue
		}

		// Verify value matches
		if !bytes.Equal(gotValue, expectedValue) {
			t.Errorf("Value mismatch for key %s: got %s, want %s", kv.key, gotValue, kv.value)
		}

		// Verify proof
		if !proof.Verify(key, gotValue) {
			t.Errorf("Proof verification failed for key %s (index %d)", kv.key, i)
			continue
		}

		// Verify root matches
		if !proof.Root.Equal(rootHash) {
			t.Errorf("Proof root mismatch for key %s: got %s, want %s",
				kv.key, proof.Root.String(), rootHash.String())
			continue
		}

		t.Logf("✓ Proof verified for key=%s, value=%s, path_length=%d",
			kv.key, kv.value, len(proof.Path))
	}
}

// TestMerkleProofWithModifiedData tests that proof verification fails for modified data
func TestMerkleProofWithModifiedData(t *testing.T) {
	// Create test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	// Create in-memory storage
	stor := storage.NewMemStorage()
	defer stor.Close()

	// Write table
	tw, err := stor.Create(storage.FileDesc{Type: storage.TypeTable, Num: 3})
	if err != nil {
		t.Fatalf("Failed to create table writer: %v", err)
	}

	o := &opt.Options{
		BlockSize:            4096,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
		BlockRestartInterval: 16,
	}

	w := NewWriter(tw, o, nil, 4096)

	for _, kv := range testData {
		if err := w.Append([]byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
	tw.Close()

	// Read table
	tr, err := stor.Open(storage.FileDesc{Type: storage.TypeTable, Num: 3})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer tr.Close()

	size, err := tr.Seek(0, 2)
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	tr.Seek(0, 0)

	r, err := NewReader(tr, size, storage.FileDesc{Type: storage.TypeTable, Num: 3}, nil, nil, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer r.Release()

	// Get value and proof for correct data
	key := []byte("key2")
	expectedValue := []byte("value2")

	_, value, proof, err := r.GetWithProof(key, nil)
	if err != nil {
		t.Fatalf("Failed to get value and proof: %v", err)
	}

	if !bytes.Equal(value, expectedValue) {
		t.Errorf("Value mismatch: got %s, want %s", value, expectedValue)
	}

	// Verify with correct data - should succeed
	if !proof.Verify(key, value) {
		t.Error("Proof verification failed for correct data")
	}

	// Try to verify with modified value - should fail
	modifiedValue := []byte("modified_value")

	// Create a fake proof with modified leaf hash
	fakeProof := &merkle.MerkleProof{
		Root:   proof.Root,
		Path:   proof.Path,
		Exists: true,
	}

	if fakeProof.Verify(key, modifiedValue) {
		t.Error("Proof verification should fail for modified data, but it succeeded")
	} else {
		t.Log("✓ Proof correctly failed for modified data")
	}

	// Verify the modified hash doesn't match
	modifiedHash := merkle.HashLeaf(key, modifiedValue)
	currentHash := modifiedHash
	for _, sibling := range proof.Path {
		if sibling.IsLeft {
			currentHash = merkle.HashInternal(sibling.Hash, currentHash)
		} else {
			currentHash = merkle.HashInternal(currentHash, sibling.Hash)
		}
	}

	if currentHash.Equal(proof.Root) {
		t.Error("Modified data should not produce the same root hash")
	} else {
		t.Log("✓ Modified data produces different root hash")
	}
}

// TestEmptyTableMerkle tests Merkle tree handling for empty tables
func TestEmptyTableMerkle(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()

	tw, err := stor.Create(storage.FileDesc{Type: storage.TypeTable, Num: 4})
	if err != nil {
		t.Fatalf("Failed to create table writer: %v", err)
	}

	o := &opt.Options{
		BlockSize:            4096,
		Compression:          opt.NoCompression,
		Comparer:             comparer.DefaultComparer,
		BlockRestartInterval: 16,
	}

	w := NewWriter(tw, o, nil, 4096)

	// Close without writing any data
	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
	tw.Close()

	// Read table
	tr, err := stor.Open(storage.FileDesc{Type: storage.TypeTable, Num: 4})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer tr.Close()

	size, err := tr.Seek(0, 2)
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	tr.Seek(0, 0)

	r, err := NewReader(tr, size, storage.FileDesc{Type: storage.TypeTable, Num: 4}, nil, nil, o)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer r.Release()

	// For empty table, Merkle root should be zero or method should handle gracefully
	rootHash, err := r.GetMerkleRoot()
	if err == nil && rootHash.IsZero() {
		t.Log("✓ Empty table has zero Merkle root")
	} else if err != nil {
		t.Logf("✓ Empty table Merkle root query returns error: %v", err)
	} else {
		t.Errorf("Empty table should have zero root or return error, got: %s", rootHash.String())
	}
}

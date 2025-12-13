// Copyright (c) 2024 BVMT Implementation
// Use of this source code is governed by a BSD-style license

package vcs

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/alinush/go-mcl"
	"github.com/syndtr/goleveldb/leveldb/merkle"
)

const (
	// Magic number for BVMT serialization format
	bvmtMagicNumber = 0x42564D5454524545 // "BVMTTREE" in ASCII

	// Version of the serialization format
	bvmtSerializationVersion = 1

	// Header size (magic + version + batch count + L + current index + total KV + reserved)
	bvmtHeaderSize = 8 + 4 + 8 + 1 + 8 + 8 + 32

	// BatchMetadata size per batch
	batchMetadataSize = 8 + 8 + 8 + 32 + 8 + 8
)

// Serialize serializes the BVMT structure to a writer
func (bvmt *BVMT) Serialize(w io.Writer) error {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	// Write header
	if err := bvmt.writeHeader(w); err != nil {
		return err
	}

	// Write batch metadata
	if err := bvmt.writeBatchMetadata(w); err != nil {
		return err
	}

	// Write batch data
	if err := bvmt.writeBatchData(w); err != nil {
		return err
	}

	// Write vector commitment data
	if err := bvmt.writeVectorCommitment(w); err != nil {
		return err
	}

	return nil
}

// writeHeader writes the BVMT header
func (bvmt *BVMT) writeHeader(w io.Writer) error {
	buf := make([]byte, bvmtHeaderSize)
	offset := 0

	// Magic number
	binary.BigEndian.PutUint64(buf[offset:], bvmtMagicNumber)
	offset += 8

	// Version
	binary.BigEndian.PutUint32(buf[offset:], bvmtSerializationVersion)
	offset += 4

	// Total batches
	binary.BigEndian.PutUint64(buf[offset:], uint64(len(bvmt.batches)))
	offset += 8

	// VBUC L parameter
	buf[offset] = bvmt.vbucL
	offset++

	// Current batch index
	binary.BigEndian.PutUint64(buf[offset:], bvmt.currentBatchIndex)
	offset += 8

	// Total KV pairs
	binary.BigEndian.PutUint64(buf[offset:], bvmt.stats.TotalKVPairs)
	offset += 8

	// Reserved (32 bytes)
	// Already zeroed in buf

	_, err := w.Write(buf)
	return err
}

// writeBatchMetadata writes metadata for all batches
func (bvmt *BVMT) writeBatchMetadata(w io.Writer) error {
	for _, batch := range bvmt.batches {
		buf := make([]byte, batchMetadataSize)
		offset := 0

		// Batch ID
		binary.BigEndian.PutUint64(buf[offset:], batch.BatchID)
		offset += 8

		// Timestamp
		binary.BigEndian.PutUint64(buf[offset:], batch.Timestamp)
		offset += 8

		// KV Pair Count
		binary.BigEndian.PutUint64(buf[offset:], uint64(len(batch.KVPairs)))
		offset += 8

		// Root Hash
		copy(buf[offset:], batch.RootHash[:])
		offset += 32

		// Data offset (placeholder, will be calculated during write)
		binary.BigEndian.PutUint64(buf[offset:], 0)
		offset += 8

		// Data size (placeholder)
		binary.BigEndian.PutUint64(buf[offset:], 0)

		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	return nil
}

// writeBatchData writes detailed data for all batches
func (bvmt *BVMT) writeBatchData(w io.Writer) error {
	for _, batch := range bvmt.batches {
		// Write KV pairs
		for _, kv := range batch.KVPairs {
			// Key length
			if err := binary.Write(w, binary.BigEndian, uint32(len(kv.Key))); err != nil {
				return err
			}
			// Key data
			if _, err := w.Write(kv.Key); err != nil {
				return err
			}

			// Value length
			if err := binary.Write(w, binary.BigEndian, uint32(len(kv.Value))); err != nil {
				return err
			}
			// Value data
			if _, err := w.Write(kv.Value); err != nil {
				return err
			}

			// Version
			if err := binary.Write(w, binary.BigEndian, kv.Version); err != nil {
				return err
			}
		}

		// Write Merkle tree structure (simplified - just store root)
		// In a full implementation, you would serialize the entire tree
		if _, err := w.Write(batch.RootHash[:]); err != nil {
			return err
		}
	}

	return nil
}

// writeVectorCommitment writes vector commitment data
func (bvmt *BVMT) writeVectorCommitment(w io.Writer) error {
	// Serialize vector digest
	digestBytes := bvmt.vectorDigest.Serialize()
	if err := binary.Write(w, binary.BigEndian, uint32(len(digestBytes))); err != nil {
		return err
	}
	if _, err := w.Write(digestBytes); err != nil {
		return err
	}

	// Note: In a full implementation, you would also serialize:
	// - BucProofAll structure
	// - auxiliaryData
	// These are complex nested structures that would require custom serialization

	return nil
}

// Deserialize deserializes BVMT from a reader
func DeserializeBVMT(r io.Reader, comparer func([]byte, []byte) int) (*BVMT, error) {
	if comparer == nil {
		comparer = compareBytes
	}

	// Read header
	header, err := readHeader(r)
	if err != nil {
		return nil, err
	}

	// Create BVMT instance
	bvmt := NewBVMT(header.vbucL, comparer)
	bvmt.currentBatchIndex = header.currentBatchIndex
	bvmt.stats.TotalKVPairs = header.totalKVPairs
	bvmt.stats.TotalBatches = header.totalBatches

	// Read batch metadata
	batchMeta, err := readBatchMetadata(r, int(header.totalBatches))
	if err != nil {
		return nil, err
	}

	// Read batch data
	batches, err := readBatchData(r, batchMeta, comparer)
	if err != nil {
		return nil, err
	}

	bvmt.batches = batches
	for _, batch := range batches {
		bvmt.batchRoots = append(bvmt.batchRoots, batch.RootHash)
	}

	// Read vector commitment
	if err := readVectorCommitment(r, bvmt); err != nil {
		return nil, err
	}

	return bvmt, nil
}

// Header information
type bvmtHeader struct {
	magic             uint64
	version           uint32
	totalBatches      uint64
	vbucL             uint8
	currentBatchIndex uint64
	totalKVPairs      uint64
}

func readHeader(r io.Reader) (*bvmtHeader, error) {
	buf := make([]byte, bvmtHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	offset := 0
	header := &bvmtHeader{}

	// Magic number
	header.magic = binary.BigEndian.Uint64(buf[offset:])
	offset += 8
	if header.magic != bvmtMagicNumber {
		return nil, ErrInvalidProof
	}

	// Version
	header.version = binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	// Total batches
	header.totalBatches = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	// VBUC L
	header.vbucL = buf[offset]
	offset++

	// Current batch index
	header.currentBatchIndex = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	// Total KV pairs
	header.totalKVPairs = binary.BigEndian.Uint64(buf[offset:])

	return header, nil
}

type batchMeta struct {
	batchID    uint64
	timestamp  uint64
	kvCount    uint64
	rootHash   merkle.Hash
	dataOffset uint64
	dataSize   uint64
}

func readBatchMetadata(r io.Reader, count int) ([]*batchMeta, error) {
	result := make([]*batchMeta, count)

	for i := 0; i < count; i++ {
		buf := make([]byte, batchMetadataSize)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		offset := 0
		meta := &batchMeta{}

		meta.batchID = binary.BigEndian.Uint64(buf[offset:])
		offset += 8

		meta.timestamp = binary.BigEndian.Uint64(buf[offset:])
		offset += 8

		meta.kvCount = binary.BigEndian.Uint64(buf[offset:])
		offset += 8

		copy(meta.rootHash[:], buf[offset:offset+32])
		offset += 32

		meta.dataOffset = binary.BigEndian.Uint64(buf[offset:])
		offset += 8

		meta.dataSize = binary.BigEndian.Uint64(buf[offset:])

		result[i] = meta
	}

	return result, nil
}

func readBatchData(r io.Reader, metadata []*batchMeta, comparer func([]byte, []byte) int) ([]*BatchMerkleTree, error) {
	batches := make([]*BatchMerkleTree, len(metadata))

	for i, meta := range metadata {
		batch := NewBatchMerkleTree(meta.batchID)
		batch.Timestamp = meta.timestamp

		// Read KV pairs
		for j := uint64(0); j < meta.kvCount; j++ {
			kv, err := readKVPair(r)
			if err != nil {
				return nil, err
			}
			batch.AddKVPair(kv)
		}

		// Read Merkle tree root (in full impl, would reconstruct tree)
		var rootHash merkle.Hash
		if _, err := io.ReadFull(r, rootHash[:]); err != nil {
			return nil, err
		}
		batch.RootHash = rootHash

		// Rebuild tree
		if err := batch.BuildTree(comparer); err != nil {
			return nil, err
		}

		batches[i] = batch
	}

	return batches, nil
}

func readKVPair(r io.Reader) (*VersionedKVPair, error) {
	var keyLen, valueLen uint32
	var version uint64

	// Read key length
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	// Read value length
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}

	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}

	// Read version
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	return NewVersionedKVPair(key, value, version), nil
}

func readVectorCommitment(r io.Reader, bvmt *BVMT) error {
	// Read vector digest length
	var digestLen uint32
	if err := binary.Read(r, binary.BigEndian, &digestLen); err != nil {
		return err
	}

	// Read vector digest
	digestBytes := make([]byte, digestLen)
	if _, err := io.ReadFull(r, digestBytes); err != nil {
		return err
	}

	// Deserialize digest
	if err := bvmt.vectorDigest.Deserialize(digestBytes); err != nil {
		return err
	}

	// CRITICAL FIX: Regenerate vector commitment from batchRoots
	// We need to rebuild the commitment and proofs to get a consistent state
	if len(bvmt.batchRoots) > 0 {
		// Convert batch roots to Fr vector
		vector := vectorToFrArray(bvmt.batchRoots)

		// Pad vector to full capacity N for VBUC
		maxCapacity := uint64(1) << bvmt.vbucL
		if uint64(len(vector)) < maxCapacity {
			padded := make([]mcl.Fr, maxCapacity)
			copy(padded, vector)
			// Zero-fill the rest
			for i := uint64(len(vector)); i < maxCapacity; i++ {
				padded[i].Clear()
			}
			vector = padded
		}

		// Regenerate proofs using OpenAll
		bvmt.proofs = bvmt.vectorCommitment.OpenAll(vector)
	}

	return nil
}

// SerializeToBytes serializes BVMT to a byte slice
func (bvmt *BVMT) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := bvmt.Serialize(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeFromBytes deserializes BVMT from a byte slice
func DeserializeFromBytes(data []byte, comparer func([]byte, []byte) int) (*BVMT, error) {
	buf := bytes.NewReader(data)
	return DeserializeBVMT(buf, comparer)
}

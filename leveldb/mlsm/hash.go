// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package mlsm

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// HashSize is the size of a hash in bytes (SHA-256)
const HashSize = 32

// Hash represents a 32-byte hash value used in Merkle trees
type Hash [HashSize]byte

// ZeroHash returns an all-zero hash
var ZeroHash = Hash{}

// NewHash creates a new Hash from a byte slice
func NewHash(b []byte) Hash {
	var h Hash
	if len(b) >= HashSize {
		copy(h[:], b[:HashSize])
	} else {
		copy(h[:], b)
	}
	return h
}

// Bytes returns the hash as a byte slice
func (h Hash) Bytes() []byte {
	return h[:]
}

// String returns the hex-encoded string representation
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// IsZero checks if the hash is all zeros
func (h Hash) IsZero() bool {
	return h == ZeroHash
}

// Equal checks if two hashes are equal
func (h Hash) Equal(other Hash) bool {
	return h == other
}

// HashData computes SHA-256 hash of data
func HashData(data []byte) Hash {
	return sha256.Sum256(data)
}

// HashConcat computes hash of concatenated data
func HashConcat(data ...[]byte) Hash {
	h := sha256.New()
	for _, d := range data {
		h.Write(d)
	}
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// HashLeaf computes hash for a leaf node (key-value pair)
// Format: Hash(0x00 || key || value)
func HashLeaf(key, value []byte) Hash {
	h := sha256.New()
	h.Write([]byte{0x00}) // Leaf marker
	h.Write(key)
	h.Write(value)
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// HashInternal computes hash for an internal node
// Format: Hash(0x01 || leftHash || rightHash)
func HashInternal(left, right Hash) Hash {
	h := sha256.New()
	h.Write([]byte{0x01}) // Internal marker
	h.Write(left[:])
	h.Write(right[:])
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// HashWithVersion computes hash including version number
// Format: Hash(0x02 || version || key || value)
func HashWithVersion(version uint64, key, value []byte) Hash {
	h := sha256.New()
	h.Write([]byte{0x02}) // Versioned leaf marker
	var versionBuf [8]byte
	binary.BigEndian.PutUint64(versionBuf[:], version)
	h.Write(versionBuf[:])
	h.Write(key)
	h.Write(value)
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// HashBlock computes hash for a data block
// This is used for hashing entire data blocks in SSTable
func HashBlock(data []byte) Hash {
	return sha256.Sum256(data)
}

// MarshalBinary implements encoding.BinaryMarshaler
func (h Hash) MarshalBinary() ([]byte, error) {
	return h[:], nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (h *Hash) UnmarshalBinary(data []byte) error {
	if len(data) != HashSize {
		return ErrInvalidHashSize
	}
	copy(h[:], data)
	return nil
}

// AggregateRoots computes an aggregated hash from multiple Merkle roots.
// This is used to create a MasterRoot that represents the entire database state.
// Format: Hash(0x03 || root1 || root2 || ... || rootN)
func AggregateRoots(roots []Hash) Hash {
	if len(roots) == 0 {
		return ZeroHash
	}

	if len(roots) == 1 {
		return roots[0]
	}

	// Use a deterministic aggregation scheme
	// Marker 0x03 for aggregated roots
	h := sha256.New()
	h.Write([]byte{0x03})

	// Write all roots in order
	for _, root := range roots {
		h.Write(root[:])
	}

	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

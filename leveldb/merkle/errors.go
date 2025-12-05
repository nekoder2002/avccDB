// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

import (
	"errors"
)

var (
	// ErrInvalidHashSize is returned when hash size is invalid
	ErrInvalidHashSize = errors.New("merkle: invalid hash size")

	// ErrInvalidProof is returned when merkle proof verification fails
	ErrInvalidProof = errors.New("merkle: invalid merkle proof")

	// ErrEmptyTree is returned when operating on an empty merkle tree
	ErrEmptyTree = errors.New("merkle: empty merkle tree")

	// ErrKeyNotFound is returned when key is not found in tree
	ErrKeyNotFound = errors.New("merkle: key not found")

	// ErrInvalidNode is returned when node structure is corrupted
	ErrInvalidNode = errors.New("merkle: invalid node structure")

	// ErrCorruptedData is returned when data is corrupted
	ErrCorruptedData = errors.New("merkle: corrupted data")

	// ErrInvalidVersion is returned when version number is invalid
	ErrInvalidVersion = errors.New("merkle: invalid version number")
)

// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package mlsm

import (
	"errors"
)

var (
	// ErrInvalidHashSize is returned when hash size is invalid
	ErrInvalidHashSize = errors.New("mlsm: invalid hash size")

	// ErrInvalidProof is returned when merkle proof verification fails
	ErrInvalidProof = errors.New("mlsm: invalid merkle proof")

	// ErrEmptyTree is returned when operating on an empty merkle tree
	ErrEmptyTree = errors.New("mlsm: empty merkle tree")

	// ErrKeyNotFound is returned when key is not found in tree
	ErrKeyNotFound = errors.New("mlsm: key not found")

	// ErrInvalidNode is returned when node structure is corrupted
	ErrInvalidNode = errors.New("mlsm: invalid node structure")

	// ErrCorruptedData is returned when data is corrupted
	ErrCorruptedData = errors.New("mlsm: corrupted data")

	// ErrInvalidVersion is returned when version number is invalid
	ErrInvalidVersion = errors.New("mlsm: invalid version number")
)

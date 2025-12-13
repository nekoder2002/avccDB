// Copyright (c) 2024 BVMT Implementation
// Use of this source code is governed by a BSD-style license

package vcs

import "errors"

// BVMT related errors
var (
	// ErrEmptyBatch is returned when attempting to add an empty batch
	ErrEmptyBatch = errors.New("vcs: batch is empty")

	// ErrInvalidVersion is returned when version number is invalid
	ErrInvalidVersion = errors.New("vcs: invalid version number")

	// ErrBatchNotFound is returned when the specified batch doesn't exist
	ErrBatchNotFound = errors.New("vcs: batch not found")

	// ErrKeyNotFound is returned when the key doesn't exist in any batch
	ErrKeyNotFound = errors.New("vcs: key not found")

	// ErrProofVerificationFailed is returned when proof verification fails
	ErrProofVerificationFailed = errors.New("vcs: proof verification failed")

	// ErrVectorCommitmentError is returned when vector commitment operation fails
	ErrVectorCommitmentError = errors.New("vcs: vector commitment operation failed")

	// ErrInvalidBatchIndex is returned when batch index is out of range
	ErrInvalidBatchIndex = errors.New("vcs: invalid batch index")

	// ErrInvalidKeyOrder is returned when keys are not in sorted order
	ErrInvalidKeyOrder = errors.New("vcs: keys must be in sorted order")

	// ErrDuplicateKey is returned when duplicate keys are found in the same batch
	ErrDuplicateKey = errors.New("vcs: duplicate key in batch")

	// ErrInvalidProof is returned when proof structure is invalid
	ErrInvalidProof = errors.New("vcs: invalid proof structure")

	// ErrBatchSizeExceeded is returned when batch size exceeds the limit
	ErrBatchSizeExceeded = errors.New("vcs: batch size exceeded")

	// ErrVectorCapacityExceeded is returned when number of batches exceeds vector capacity
	ErrVectorCapacityExceeded = errors.New("vcs: vector capacity exceeded")
)

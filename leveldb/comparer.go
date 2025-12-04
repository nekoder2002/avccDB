// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/comparer"
)

type iComparer struct {
	ucmp comparer.Comparer
}

func (icmp *iComparer) uName() string {
	return icmp.ucmp.Name()
}

func (icmp *iComparer) uCompare(a, b []byte) int {
	return icmp.ucmp.Compare(a, b)
}

func (icmp *iComparer) uSeparator(dst, a, b []byte) []byte {
	return icmp.ucmp.Separator(dst, a, b)
}

func (icmp *iComparer) uSuccessor(dst, b []byte) []byte {
	return icmp.ucmp.Successor(dst, b)
}

func (icmp *iComparer) Name() string {
	return icmp.uName()
}

func (icmp *iComparer) Compare(a, b []byte) int {
	// Check if keys have version (length >= 16)
	hasVerA := hasVersion(a)
	hasVerB := hasVersion(b)

	var ukeyA, ukeyB []byte
	var versionA, versionB uint64
	var seqA, seqB uint64

	if hasVerA {
		ukeyA, versionA, seqA, _, _ = parseInternalKeyWithVersion(a)
	} else {
		ukeyA, seqA, _, _ = parseInternalKey(a)
		versionA = 0
	}

	if hasVerB {
		ukeyB, versionB, seqB, _, _ = parseInternalKeyWithVersion(b)
	} else {
		ukeyB, seqB, _, _ = parseInternalKey(b)
		versionB = 0
	}

	// First compare user keys
	x := icmp.uCompare(ukeyA, ukeyB)
	if x == 0 {
		// Same user key
		// Special case: if either version is keyMaxSeq, compare by sequence only
		// This ensures when querying with keyMaxSeq, we find the entry with max seq
		if versionA == keyMaxSeq || versionB == keyMaxSeq {
			// keyMaxSeq is a wildcard - compare by sequence to find the latest
			// When both are keyMaxSeq, they're equal
			if versionA == keyMaxSeq && versionB == keyMaxSeq {
				// Both are wildcards, compare by sequence
				if seqA > seqB {
					return -1
				} else if seqA < seqB {
					return 1
				}
				return 0
			}
			// One is keyMaxSeq, one is actual version
			// keyMaxSeq (query key) should sort before actual versions for Seek to work
			// Since versions are descending, keyMaxSeq > any actual version
			if versionA == keyMaxSeq {
				return -1 // A (keyMaxSeq) < B (actual version), so A comes first
			} else {
				return 1 // B (keyMaxSeq) < A (actual version), so B comes first
			}
		}
		// Normal case: compare version (descending - higher version first)
		if versionA > versionB {
			return -1
		} else if versionA < versionB {
			return 1
		}
		// Same version, compare sequence (descending - higher seq first)
		if seqA > seqB {
			return -1
		} else if seqA < seqB {
			return 1
		}
	}
	return x
}

func (icmp *iComparer) Separator(dst, a, b []byte) []byte {
	ua, ub := internalKey(a).ukey(), internalKey(b).ukey()
	dst = icmp.uSeparator(dst, ua, ub)
	if dst != nil && len(dst) < len(ua) && icmp.uCompare(ua, dst) < 0 {
		// Append earliest possible number.
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}

func (icmp *iComparer) Successor(dst, b []byte) []byte {
	ub := internalKey(b).ukey()
	dst = icmp.uSuccessor(dst, ub)
	if dst != nil && len(dst) < len(ub) && icmp.uCompare(ub, dst) < 0 {
		// Append earliest possible number.
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}

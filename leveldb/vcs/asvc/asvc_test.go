package asvc

import (
	"testing"

	"github.com/alinush/go-mcl"
	"github.com/syndtr/goleveldb/leveldb/vcs/fft"
	"github.com/syndtr/goleveldb/leveldb/vcs/utils"
)

func init() {
	mcl.InitFromString("bls12-381")
	fft.InitGlobals()
}

// TestAsvcCommitOpenAll 测试 ASVC 的 Commit 和 OpenAll 功能
func TestAsvcCommitOpenAll(t *testing.T) {
	t.Log("Testing ASVC Commit and OpenAll")
	for L := 8; L <= 12; L++ {
		t.Run("L="+string(rune(L+48)), func(t *testing.T) {
			vc := ASVC{}
			l := uint8(L)
			n := 1 << l
			vc.Init(l, []mcl.G1{}, []mcl.G2{})

			vec := make([]mcl.Fr, n)
			for i := 0; i < n; i++ {
				vec[i].Random()
			}

			// Test Commit
			digest := vc.Commit(vec)
			if digest.IsZero() {
				t.Errorf("Commit returned zero digest for L=%d", L)
			}

			// Test OpenAll
			proofs := vc.OpenAll(vec)
			if len(proofs) != n {
				t.Errorf("OpenAll returned %d proofs, expected %d", len(proofs), n)
			}

			// Verify a few random positions
			for i := 0; i < 5 && i < n; i++ {
				val := Val{Index: uint64(i), Y: vec[i]}
				if !vc.VerifySingle(digest, proofs[i], val) {
					t.Errorf("Failed to verify proof at index %d for L=%d", i, L)
				}
			}
			t.Logf("L=%d: Successfully tested Commit and OpenAll with %d elements", L, n)
		})
	}
}

// TestAsvcOpen 测试单个位置的 Open 功能
func TestAsvcOpen(t *testing.T) {
	L := uint8(8)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	digest := vc.Commit(vec)

	// Test opening specific indices
	testIndices := []uint64{0, 1, uint64(n / 2), uint64(n - 1)}
	for _, idx := range testIndices {
		proof := vc.Open(idx, vec)
		val := Val{Index: idx, Y: vec[idx]}
		if !vc.VerifySingle(digest, proof, val) {
			t.Errorf("Failed to verify proof at index %d", idx)
		}
	}
	t.Log("Successfully tested Open for individual positions")
}

// TestAsvcUpdateCommitment 测试更新承诺功能
func TestAsvcUpdateCommitment(t *testing.T) {
	L := uint8(8)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	digest := vc.Commit(vec)

	// Update a value
	updateIdx := uint64(5)
	var delta mcl.Fr
	delta.Random()
	req := UpdateReq{Index: updateIdx, Delta: delta}

	// Update commitment
	newDigest := vc.UpdateCommitment(digest, req)

	// Update the vector
	var newVal mcl.Fr
	mcl.FrAdd(&newVal, &vec[updateIdx], &delta)
	vec[updateIdx] = newVal

	// Verify new commitment matches
	expectedDigest := vc.Commit(vec)
	if !newDigest.IsEqual(&expectedDigest) {
		t.Error("Updated commitment doesn't match expected commitment")
	}
	t.Log("Successfully tested UpdateCommitment")
}

// TestAsvcUpdateProof 测试更新证明功能
func TestAsvcUpdateProof(t *testing.T) {
	L := uint8(8)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	digest := vc.Commit(vec)
	proofs := vc.OpenAll(vec)

	// Update a value
	updateIdx := uint64(10)
	verifyIdx := uint64(5)
	var delta mcl.Fr
	delta.Random()
	req := UpdateReq{Index: updateIdx, Delta: delta}

	// Update commitment and proof
	newDigest := vc.UpdateCommitment(digest, req)
	newProof := vc.UpdateProof(proofs[verifyIdx], verifyIdx, req)

	// Verify the updated proof
	val := Val{Index: verifyIdx, Y: vec[verifyIdx]}
	if !vc.VerifySingle(newDigest, newProof, val) {
		t.Error("Failed to verify updated proof")
	}
	t.Log("Successfully tested UpdateProof")
}

// TestAsvcAggregate 测试证明聚合功能
func TestAsvcAggregate(t *testing.T) {
	L := uint8(8)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	digest := vc.Commit(vec)
	proofs := vc.OpenAll(vec)

	// Select some indices to aggregate
	numAgg := 10
	aggs := make([]Inp, numAgg)
	aggvs := make([]Val, numAgg)
	indices := utils.GenerateIndices(uint64(n), numAgg)

	for i := 0; i < numAgg; i++ {
		idx := indices[i]
		aggs[i] = Inp{Index: idx, Proof: proofs[idx]}
		aggvs[i] = Val{Index: idx, Y: vec[idx]}
	}

	// Aggregate proofs
	aggProof := vc.Aggregate(aggs)

	// Verify aggregated proof
	if !vc.VerifyAggregation(digest, aggProof, aggvs) {
		t.Error("Failed to verify aggregated proof")
	}
	t.Log("Successfully tested Aggregate and VerifyAggregation")
}

// TestAsvcAggFake 测试 Fake 版本的聚合功能
func TestAsvcAggFake(t *testing.T) {
	for L := 20; L <= 22; L += 2 {
		t.Run("L="+string(rune(L+48)), func(t *testing.T) {
			a := ASVC{}
			l := uint8(L)
			a.Init_Fake(l)

			vec := make([]mcl.Fr, 2)
			vec[0].Random()
			vec[1].Random()

			var proof mcl.G1
			proof.Random()

			numAgg := 100
			aggs := make([]Inp, numAgg)
			aggvs := make([]Val, numAgg)
			lis := utils.GenerateIndices(uint64(1<<a.L), numAgg)

			for j := 0; j < numAgg; j++ {
				id := lis[j]
				aggs[j] = Inp{Index: id, Proof: proof}
				aggvs[j] = Val{Index: id, Y: vec[id&1]}
			}

			// Test Aggregate_Fake
			aggProof := a.Aggregate_Fake(aggs)
			if aggProof.IsZero() {
				t.Error("Aggregate_Fake returned zero proof")
			}

			var digest mcl.G1
			digest.Random()

			// Note: VerifyAggregation_Fake is a mock function, so we just test it runs
			_ = a.VerifyAggregation_Fake(digest, proof, aggvs)
			t.Logf("L=%d: Successfully tested Aggregate_Fake with %d aggregations", L, numAgg)
		})
	}
}

// TestAsvcIndividualFake 测试 Fake 版本的单个验证功能
func TestAsvcIndividualFake(t *testing.T) {
	for L := 20; L <= 22; L += 2 {
		t.Run("L="+string(rune(L+48)), func(t *testing.T) {
			vc := ASVC{}
			vc.Init_Fake(uint8(L))

			numTests := 10
			for i := 0; i < numTests; i++ {
				var digest mcl.G1
				digest.Random()
				var v mcl.Fr
				v.Random()
				var Pi mcl.G1
				Pi.Random()

				// VerifySingle_Fake is a mock function, just test it runs
				_ = vc.VerifySingle_Fake(digest, Pi, Val{
					Index: 0,
					Y:     v,
				})
			}
			t.Logf("L=%d: Successfully tested VerifySingle_Fake with %d iterations", L, numTests)
		})
	}
}

// BenchmarkAsvcCommit 性能测试：Commit 操作
func BenchmarkAsvcCommit(b *testing.B) {
	L := uint8(10)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.Commit(vec)
	}
}

// BenchmarkAsvcOpenAll 性能测试：OpenAll 操作
func BenchmarkAsvcOpenAll(b *testing.B) {
	L := uint8(10)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.OpenAll(vec)
	}
}

// BenchmarkAsvcAggregate 性能测试：Aggregate 操作
func BenchmarkAsvcAggregate(b *testing.B) {
	L := uint8(10)
	vc := ASVC{}
	n := 1 << L
	vc.Init(L, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}

	proofs := vc.OpenAll(vec)
	numAgg := 10
	aggs := make([]Inp, numAgg)
	indices := utils.GenerateIndices(uint64(n), numAgg)

	for i := 0; i < numAgg; i++ {
		idx := indices[i]
		aggs[i] = Inp{Index: idx, Proof: proofs[idx]}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.Aggregate(aggs)
	}
}

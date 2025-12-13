package asvc

import (
	"fmt"
	"github.com/alinush/go-mcl"
	"math"
	"math/rand"
	"time"
)

// BasicVertify_Block_Existence 存在证明验证时间，需要重新组织，考虑用update获取更新证明、签名，证明验证是对的吗？
func BasicVertify_Block_Existence(merkleRoots [][]byte) {
	//fmt.Println("BalanceProofs/asvc Verify individual time:")
	fmt.Println("BasicVertify Block Existence time:")

	n := len(merkleRoots)
	L := int(math.Ceil(math.Log2(float64(n))))
	//println(L)

	vc := ASVC{}
	//vc.Init_Fake(uint8(L))
	vc.Init(uint8(L), []mcl.G1{}, []mcl.G2{})
	vec := make([]mcl.Fr, n, n)
	for i := 0; i < n; i++ {
		vec[i].SetHashOf(merkleRoots[i][:])
	}
	dt := time.Now()
	digest := vc.Commit(vec)
	duration := time.Since(dt)
	fmt.Printf("Commit time: %f sec\n", duration.Seconds())
	dt = time.Now()

	proofs := vc.OpenAll(vec)

	duration = time.Since(dt)
	fmt.Printf("OpenAll time: %f sec\n", duration.Seconds())
	t := 0.0
	for i := 0; i < n; i++ {
		id := rand.Uint64() % uint64(n)

		aggvs := Val{Index: id, Y: vec[id]}

		dt := time.Now()
		vc.VerifySingle(digest, proofs[id], aggvs)
		duration := time.Since(dt)
		t = t + duration.Seconds()
	}

	fmt.Printf("Verify ind time: %f sec\n", t/float64(n))
}

// BasicVertify_Block_No_Existence 不存在证明验证时间，需要重新组织，考虑用update获取更新证明、签名，证明验证是对的吗？
func BasicVertify_Block_No_Existence(merkleRoots [][]byte) {
	//fmt.Println("BalanceProofs/asvc Verify individual time:")
	fmt.Println("BasicVertify Block No Existence time:")

	n := len(merkleRoots)
	L := int(math.Ceil(math.Log2(float64(n))))
	//println(L)

	vc := ASVC{}
	//vc.Init_Fake(uint8(L))
	vc.Init(uint8(L), []mcl.G1{}, []mcl.G2{})
	vec := make([]mcl.Fr, n, n)
	for i := 0; i < n; i++ {
		vec[i].SetHashOf(merkleRoots[i][:])
	}
	dt := time.Now()
	digest := vc.Commit(vec)
	duration := time.Since(dt)
	fmt.Printf("Commit time: %f sec\n", duration.Seconds())
	dt = time.Now()

	vc.OpenAll(vec)

	duration = time.Since(dt)
	fmt.Printf("OpenAll time: %f sec\n", duration.Seconds())
	t := 0.0
	for i := 0; i < n; i++ {
		id := rand.Uint64() % uint64(n)

		aggvs := Val{Index: id, Y: vec[id]}

		var randProof mcl.G1
		randProof.Random()

		dt := time.Now()
		vc.VerifySingle(digest, randProof, aggvs)
		duration := time.Since(dt)
		t = t + duration.Seconds()
	}

	fmt.Printf("Verify ind time: %f sec\n", t/float64(n))
}

// Basic_AsvcAgg_Vertify_Extence 存在证明聚合以及验证时间
func Basic_AsvcAgg_Vertify_Extence(merkleRoots [][]byte) {
	fmt.Println("BalanceProofs/asvc Agg and Verify agg time:")

	n := len(merkleRoots)
	L := int(math.Ceil(math.Log2(float64(n))))

	a := ASVC{}

	l := uint8(L)

	a.Init(l, []mcl.G1{}, []mcl.G2{})
	vec := make([]mcl.Fr, n, n)
	for i := 0; i < n; i++ {
		vec[i].SetHashOf(merkleRoots[i][:])
	}
	digest := a.Commit(vec)

	proofs := a.OpenAll(vec)
	aggs := make([]Inp, n)
	aggvs := make([]Val, n)

	for j := 0; j < n; j++ {
		aggs[j] = Inp{Index: uint64(j), Proof: proofs[j]}
		aggvs[j] = Val{Index: uint64(j), Y: vec[j]}
	}
	dt := time.Now()
	proof := a.Aggregate(aggs)
	duration := time.Since(dt)
	fmt.Printf("Agg time: %f sec\t", duration.Seconds())

	dt = time.Now()

	a.VerifyAggregation(digest, proof, aggvs)
	//println(result)

	duration = time.Since(dt)
	fmt.Printf("Verify agg time: %f sec\n", duration.Seconds())
}

// Basic_Agg_ProofSize 计算聚合证明大小，这部分逻辑需要仔细考量一下
func Basic_Agg_ProofSize(merkleRoots [][]byte) {
	fmt.Println("Aggregate ProofSize:")

	n := len(merkleRoots)
	L := int(math.Ceil(math.Log2(float64(n))))
	//println(L)

	vc := ASVC{}

	vc.Init(uint8(L), []mcl.G1{}, []mcl.G2{})
	vec := make([]mcl.Fr, n, n)
	for i := 0; i < n; i++ {
		vec[i].SetHashOf(merkleRoots[i][:])
	}
	vc.Commit(vec)

	proofs := vc.OpenAll(vec)
	aggs := make([]Inp, n)
	aggvs := make([]Val, n)

	for j := 0; j < n; j++ {
		aggs[j] = Inp{Index: uint64(j), Proof: proofs[j]}
		aggvs[j] = Val{Index: uint64(j), Y: vec[j]}
	}
	proof := vc.Aggregate(aggs)

	fmt.Printf("proof size: %d bytes\n", len(proof.Serialize()))
}

// TestCommit 用于测试什么情况下单个证明验证能通过
func TestCommit() {
	L := 3
	//fmt.Printf("L=%d\t", L)
	vc := ASVC{}
	//
	l := uint8(L)
	//n := 2
	n := 1 << l
	vc.Init(l, []mcl.G1{}, []mcl.G2{})

	vec := make([]mcl.Fr, n, n)
	for i := 0; i < n; i++ {
		vec[i].Random()
	}
	dt := time.Now()
	digest := vc.Commit(vec)
	//digest2 := digest
	//println(digest.IsEqual(&digest2))
	println(digest.Serialize())

	duration := time.Since(dt)
	fmt.Printf("Commit time: %f sec\n", duration.Seconds())

	dt = time.Now()
	//proofs := make([]mcl.Fr, n)
	proofs := vc.OpenAll(vec)
	//proofs := vc.OpenAll_Fake(vec)
	//proof := vc.Open(1, vec)
	duration = time.Since(dt)
	aggvs := Val{Index: 0, Y: vec[0]}
	//vc.Open(0, vector []mcl.Fr)
	println(vc.VerifySingle(digest, proofs[0], aggvs))
	fmt.Printf("OpenAll time: %f sec\n", duration.Seconds())
}

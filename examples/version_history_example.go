// mLSM 溯源查询示例
// 展示如何使用 GetVersionHistory API 进行历史数据追溯

package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func main() {
	// 打开数据库
	stor := storage.NewMemStorage()
	defer stor.Close()

	db, err := leveldb.Open(stor, nil)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("=== mLSM 溯源查询示例 ===\n")

	// 场景1：区块链状态追溯
	// 模拟用户账户在不同区块的状态变化
	key := []byte("account:0x1234")

	// 区块 100: 初始余额
	db.PutWithVersion(key, []byte(`{"balance": 1000, "nonce": 0}`), 100, nil)

	// 区块 150: 转账支出
	db.PutWithVersion(key, []byte(`{"balance": 800, "nonce": 1}`), 150, nil)

	// 区块 200: 收款
	db.PutWithVersion(key, []byte(`{"balance": 1200, "nonce": 1}`), 200, nil)

	// 区块 250: 智能合约调用
	db.PutWithVersion(key, []byte(`{"balance": 1100, "nonce": 2}`), 250, nil)

	// 区块 300: 再次转账
	db.PutWithVersion(key, []byte(`{"balance": 900, "nonce": 3}`), 300, nil)

	fmt.Println("场景1: 区块链账户状态追溯")
	fmt.Println("账户:", string(key))
	fmt.Println()

	// 查询1: 获取账户的完整历史
	fmt.Println("查询1: 账户的完整历史")
	entries, err := db.GetVersionHistory(key, 0, 0, nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	for _, entry := range entries {
		fmt.Printf("  区块 %d: %s\n", entry.Version, entry.Value)
	}
	fmt.Println()

	// 查询2: 查询特定区块范围 [150, 250]
	fmt.Println("查询2: 区块 150-250 之间的状态变化")
	entries, err = db.GetVersionHistory(key, 150, 250, nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	for _, entry := range entries {
		fmt.Printf("  区块 %d: %s\n", entry.Version, entry.Value)
	}
	fmt.Println()

	// 查询3: 审计查询 - 从某个区块开始的所有变更
	fmt.Println("查询3: 区块 200 之后的所有变更（审计用途）")
	entries, err = db.GetVersionHistory(key, 200, 0, nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	for _, entry := range entries {
		fmt.Printf("  区块 %d: %s\n", entry.Version, entry.Value)
	}
	fmt.Println()

	// 场景2: 多账户历史查询
	fmt.Println("场景2: 多账户历史追溯")
	accounts := []struct {
		address string
		blocks  []uint64
		states  []string
	}{
		{
			address: "account:alice",
			blocks:  []uint64{100, 200, 300},
			states: []string{
				`{"balance": 5000, "nonce": 0}`,
				`{"balance": 4500, "nonce": 1}`,
				`{"balance": 5500, "nonce": 2}`,
			},
		},
		{
			address: "account:bob",
			blocks:  []uint64{150, 250},
			states: []string{
				`{"balance": 2000, "nonce": 0}`,
				`{"balance": 2500, "nonce": 1}`,
			},
		},
	}

	// 写入多账户数据
	for _, acc := range accounts {
		for i, block := range acc.blocks {
			key := []byte(acc.address)
			value := []byte(acc.states[i])
			db.PutWithVersion(key, value, block, nil)
		}
	}

	// 查询每个账户的历史
	for _, acc := range accounts {
		key := []byte(acc.address)
		entries, err := db.GetVersionHistory(key, 0, 0, nil)
		if err != nil {
			log.Printf("  Query %s failed: %v", acc.address, err)
			continue
		}

		fmt.Printf("\n%s 历史:\n", acc.address)
		for _, entry := range entries {
			fmt.Printf("  区块 %d: %s\n", entry.Version, entry.Value)
		}
	}
	fmt.Println()

	// 场景3: 时间范围查询（用于回滚测试）
	fmt.Println("场景3: 状态回滚模拟")
	fmt.Println("假设需要回滚到区块 200 之前的状态")

	testKey := []byte("account:0x1234")
	entries, err = db.GetVersionHistory(testKey, 0, 200, nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Println("回滚前的状态历史:")
	for _, entry := range entries {
		fmt.Printf("  区块 %d: %s\n", entry.Version, entry.Value)
	}

	if len(entries) > 0 {
		latest := entries[len(entries)-1]
		fmt.Printf("\n回滚到的状态（区块 %d）:\n  %s\n", latest.Version, latest.Value)
	}
	fmt.Println()

	// 统计信息
	fmt.Println("=== 溯源查询统计 ===")
	allEntries, _ := db.GetVersionHistory([]byte("account:0x1234"), 0, 0, nil)
	fmt.Printf("account:0x1234 总共有 %d 个历史版本\n", len(allEntries))

	if len(allEntries) > 0 {
		fmt.Printf("最早版本: 区块 %d\n", allEntries[0].Version)
		fmt.Printf("最新版本: 区块 %d\n", allEntries[len(allEntries)-1].Version)
		fmt.Printf("版本跨度: %d 个区块\n",
			allEntries[len(allEntries)-1].Version-allEntries[0].Version)
	}

	fmt.Println("\n✓ 溯源查询示例完成")
}

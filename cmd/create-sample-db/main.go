// 创建一个示例数据库用于测试 inspector 工具

package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	dbPath := "testdb"

	// 打开数据库
	stor, err := storage.OpenFile(dbPath, false)
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer stor.Close()

	db, err := leveldb.Open(stor, &opt.Options{
		WriteBuffer: 1024 * 1024,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("创建示例数据库...")

	// 写入一些普通数据
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user-%03d", i)
		value := fmt.Sprintf("User %d data with some content", i)
		if err := db.Put([]byte(key), []byte(value), nil); err != nil {
			log.Fatalf("Put failed: %v", err)
		}
	}

	// 写入一些版本化数据
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("item-%03d", i)
		for v := 1; v <= 3; v++ {
			value := fmt.Sprintf("Item %d version %d data", i, v)
			if err := db.PutWithVersion([]byte(key), []byte(value), uint64(v), nil); err != nil {
				log.Fatalf("PutWithVersion failed: %v", err)
			}
		}
	}

	// 强制刷新到磁盘
	fmt.Println("强制 flush 数据...")
	if err := db.CompactRange(util.Range{}); err != nil {
		fmt.Printf("Warning: CompactRange: %v\n", err)
	}

	fmt.Printf("✓ 数据库创建完成: %s\n", dbPath)
	fmt.Printf("  - 100 个普通键\n")
	fmt.Printf("  - 50 个版本化键 (每个3个版本)\n")
	fmt.Printf("\n使用以下命令查看:\n")
	fmt.Printf("  .\\leveldb-inspector.exe -db=%s\n", dbPath)
	fmt.Printf("  .\\leveldb-inspector.exe -db=%s -keys -limit=30\n", dbPath)
	fmt.Printf("  .\\leveldb-inspector.exe -db=%s -prefix=user-\n", dbPath)
}

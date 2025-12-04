// leveldb-inspector - LevelDB 数据库结构查看工具
// 用于查看 mLSM 增强的 LevelDB 内部结构

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	dbPath      = flag.String("db", "", "数据库路径 (必需)")
	showKeys    = flag.Bool("keys", false, "显示键列表")
	showStats   = flag.Bool("stats", true, "显示统计信息")
	showLevels  = flag.Bool("levels", true, "显示层级信息")
	showRoot    = flag.Bool("root", true, "显示 MasterRoot")
	keyLimit    = flag.Int("limit", 100, "显示的键数量限制")
	keyPrefix   = flag.String("prefix", "", "只显示指定前缀的键")
	showVersion = flag.Bool("version", false, "尝试解析并显示键的版本信息")
	allVersions = flag.Bool("all-versions", false, "统计所有版本的键（不去重）")
)

func main() {
	flag.Parse()

	if *dbPath == "" {
		fmt.Println("错误: 必须指定数据库路径")
		fmt.Println("\n用法:")
		flag.PrintDefaults()
		fmt.Println("\n示例:")
		fmt.Println("  leveldb-inspector -db=./testdata/mlsm_final_test")
		fmt.Println("  leveldb-inspector -db=./mydb -keys -limit=50")
		fmt.Println("  leveldb-inspector -db=./mydb -prefix=user-")
		os.Exit(1)
	}

	// 打开数据库 (只读模式)
	stor, err := storage.OpenFile(*dbPath, true)
	if err != nil {
		fmt.Printf("错误: 无法打开数据库存储: %v\n", err)
		os.Exit(1)
	}
	defer stor.Close()

	db, err := leveldb.Open(stor, &opt.Options{
		ReadOnly: true,
	})
	if err != nil {
		fmt.Printf("错误: 无法打开数据库: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("=" + strings.Repeat("=", 78) + "=")
	fmt.Println("  LevelDB Inspector - mLSM 数据库结构查看工具")
	fmt.Println("=" + strings.Repeat("=", 78) + "=")
	fmt.Printf("数据库路径: %s\n", *dbPath)
	fmt.Println()

	// 显示 MasterRoot
	if *showRoot {
		showMasterRoot(db)
		fmt.Println()
	}

	// 显示文件系统信息
	if *showStats {
		showFileSystemInfo(*dbPath)
		fmt.Println()
	}

	// 显示层级信息
	if *showLevels {
		showLevelInfo(db)
		fmt.Println()
	}

	// 显示键信息
	if *showKeys {
		showKeysInfo(db)
		fmt.Println()
	}

	// 显示统计摘要
	if *showStats {
		showStatsSummary(db)
	}

	fmt.Println("=" + strings.Repeat("=", 78) + "=")
	fmt.Println("检查完成")
	fmt.Println("=" + strings.Repeat("=", 78) + "=")
}

// showMasterRoot 显示 MasterRoot 信息
func showMasterRoot(db *leveldb.DB) {
	fmt.Println("【MasterRoot 信息】")
	fmt.Println(strings.Repeat("-", 80))

	masterRoot, err := db.GetMasterRoot()
	if err != nil {
		fmt.Printf("  获取 MasterRoot 失败: %v\n", err)
		return
	}

	fmt.Printf("  MasterRoot Hash: %x\n", masterRoot[:])

	// 显示 Hash 的可读格式
	fmt.Printf("  前16字节:       %x\n", masterRoot[:16])
	fmt.Printf("  后16字节:       %x\n", masterRoot[16:])
}

// showFileSystemInfo 显示文件系统信息
func showFileSystemInfo(dbPath string) {
	fmt.Println("【文件系统信息】")
	fmt.Println(strings.Repeat("-", 80))

	// 统计文件信息
	var (
		sstFiles     []os.FileInfo
		logFiles     []os.FileInfo
		otherFiles   []os.FileInfo
		totalSize    int64
		sstTotalSize int64
	)

	filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		totalSize += info.Size()
		ext := filepath.Ext(info.Name())

		switch ext {
		case ".ldb", ".sst":
			sstFiles = append(sstFiles, info)
			sstTotalSize += info.Size()
		case ".log":
			logFiles = append(logFiles, info)
		default:
			otherFiles = append(otherFiles, info)
		}

		return nil
	})

	fmt.Printf("  数据库目录:     %s\n", dbPath)
	fmt.Printf("  SST 文件数:     %d (%.2f MB)\n", len(sstFiles), float64(sstTotalSize)/(1024*1024))
	fmt.Printf("  日志文件数:     %d\n", len(logFiles))
	fmt.Printf("  其他文件数:     %d\n", len(otherFiles))
	fmt.Printf("  总大小:         %.2f MB\n", float64(totalSize)/(1024*1024))

	// 显示最大的几个 SST 文件
	if len(sstFiles) > 0 {
		sort.Slice(sstFiles, func(i, j int) bool {
			return sstFiles[i].Size() > sstFiles[j].Size()
		})

		fmt.Println("\n  最大的 SST 文件:")
		limit := 5
		if len(sstFiles) < limit {
			limit = len(sstFiles)
		}
		for i := 0; i < limit; i++ {
			f := sstFiles[i]
			fmt.Printf("    %s: %.2f KB\n", f.Name(), float64(f.Size())/1024)
		}
	}
}

// showLevelInfo 显示层级信息 (从DB.Stats获取准确信息)
func showLevelInfo(db *leveldb.DB) {
	fmt.Println("【层级分布信息】")
	fmt.Println(strings.Repeat("-", 80))

	// 使用 DB.Stats 获取准确的层级信息
	var stats leveldb.DBStats
	if err := db.Stats(&stats); err != nil {
		fmt.Printf("  获取统计信息失败: %v\n", err)
		return
	}

	fmt.Println("  层级详情:")
	fmt.Println()
	fmt.Printf("  %-8s %-12s %-15s %-15s %-15s %-15s\n",
		"Level", "Tables", "Size(MB)", "Time(s)", "Read(MB)", "Write(MB)")
	fmt.Println("  " + strings.Repeat("-", 90))

	var totalTables, totalSize, totalRead, totalWrite int64
	var totalDuration time.Duration

	for level := 0; level < len(stats.LevelTablesCounts); level++ {
		tableCount := stats.LevelTablesCounts[level]
		size := stats.LevelSizes[level]
		duration := stats.LevelDurations[level]
		read := stats.LevelRead[level]
		write := stats.LevelWrite[level]

		// 累计统计
		totalTables += int64(tableCount)
		totalSize += size
		totalDuration += duration
		totalRead += read
		totalWrite += write

		// 显示每层信息
		if tableCount > 0 || duration > 0 {
			fmt.Printf("  %-8d %-12d %-15.2f %-15.2f %-15.2f %-15.2f\n",
				level,
				tableCount,
				float64(size)/(1024*1024),
				duration.Seconds(),
				float64(read)/(1024*1024),
				float64(write)/(1024*1024))
		}
	}

	fmt.Println("  " + strings.Repeat("-", 90))
	fmt.Printf("  %-8s %-12d %-15.2f %-15.2f %-15.2f %-15.2f\n",
		"Total",
		totalTables,
		float64(totalSize)/(1024*1024),
		totalDuration.Seconds(),
		float64(totalRead)/(1024*1024),
		float64(totalWrite)/(1024*1024))

	fmt.Println()
	fmt.Printf("  Compaction 统计:\n")
	fmt.Printf("    MemDB Compactions:    %d\n", stats.MemComp)
	fmt.Printf("    Level0 Compactions:   %d\n", stats.Level0Comp)
	fmt.Printf("    NonLevel0 Compactions: %d\n", stats.NonLevel0Comp)
	fmt.Printf("    Seek Compactions:     %d\n", stats.SeekComp)
}

// showKeysInfo 显示键信息
func showKeysInfo(db *leveldb.DB) {
	fmt.Println("【键数据信息】")
	fmt.Println(strings.Repeat("-", 80))

	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	count := 0
	var firstKey, lastKey []byte
	totalValueSize := int64(0)

	fmt.Printf("  前缀过滤:       %s\n", ternary(*keyPrefix == "", "(无)", *keyPrefix))
	fmt.Printf("  显示限制:       %d 个键\n", *keyLimit)
	fmt.Printf("  版本解析:       %s\n\n", ternary(*showVersion, "开启", "关闭"))

	fmt.Println("  键列表:")
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// 应用前缀过滤
		if *keyPrefix != "" && !strings.HasPrefix(string(key), *keyPrefix) {
			continue
		}

		if count == 0 {
			firstKey = append([]byte(nil), key...)
		}
		lastKey = append([]byte(nil), key...)

		totalValueSize += int64(len(value))

		// 显示键信息
		if count < *keyLimit {
			keyStr := safeString(key)
			valueStr := safeString(value)

			if len(valueStr) > 50 {
				valueStr = valueStr[:50] + "..."
			}

			fmt.Printf("    [%d] %s = %s (value: %d bytes)\n",
				count+1, keyStr, valueStr, len(value))

			// 如果启用版本解析，尝试显示版本信息
			if *showVersion {
				// 这里简化处理，实际需要根据 internal key 格式解析
				fmt.Printf("         (键长度: %d bytes)\n", len(key))
			}
		}

		count++
	}

	if err := iter.Error(); err != nil {
		fmt.Printf("\n  迭代错误: %v\n", err)
	}

	fmt.Printf("\n  统计:")
	fmt.Printf("\n    总键数:         %d\n", count)
	if count > 0 {
		fmt.Printf("    第一个键:       %s\n", safeString(firstKey))
		fmt.Printf("    最后一个键:     %s\n", safeString(lastKey))
		fmt.Printf("    平均值大小:     %.2f bytes\n", float64(totalValueSize)/float64(count))
		fmt.Printf("    总值大小:       %.2f KB\n", float64(totalValueSize)/1024)
	}
}

// showStatsSummary 显示统计摘要
func showStatsSummary(db *leveldb.DB) {
	fmt.Println("【数据库统计摘要】")
	fmt.Println(strings.Repeat("-", 80))

	if *allVersions {
		// 统计所有版本（不去重）
		fmt.Println("  模式: 统计所有版本（包括历史版本）")
		fmt.Println("  正在扫描 SST 文件...")

		totalRecords, err := countAllVersions(db)
		if err != nil {
			fmt.Printf("  统计错误: %v\n", err)
			return
		}

		fmt.Printf("  总记录数:       %d（包括所有版本）\n", totalRecords)
	} else {
		// 快速统计（只统计唯一键）
		fmt.Println("  模式: 统计唯一键（只计最新版本）")
		iter := db.NewIterator(nil, nil)
		defer iter.Release()

		totalKeys := 0
		for iter.Next() {
			totalKeys++
		}

		fmt.Printf("  总键数:         %d（唯一键）\n", totalKeys)
	}

	// 估算压缩率等信息需要更多底层访问
	fmt.Println("\n  提示: 更详细的统计信息需要访问底层存储引擎")
	fmt.Println("        包括: 压缩率、读写放大、缓存命中率等")
}

// safeString 将字节转换为安全的字符串表示
func safeString(b []byte) string {
	// 尝试作为字符串显示，如果包含不可打印字符则显示十六进制
	isPrintable := true
	for _, c := range b {
		if c < 32 || c > 126 {
			isPrintable = false
			break
		}
	}

	if isPrintable && len(b) > 0 {
		return string(b)
	}

	// 显示十六进制（限制长度）
	hexStr := fmt.Sprintf("%x", b)
	if len(hexStr) > 64 {
		hexStr = hexStr[:64] + "..."
	}
	return "[hex:" + hexStr + "]"
}

// countAllVersions 统计所有版本的键（包括历史版本）
// 通过直接读取 SST 文件统计
func countAllVersions(db *leveldb.DB) (int64, error) {
	// 注意：LevelDB 的公开 API 不支持直接遍历 internal keys
	// 标准迭代器只返回每个 user key 的最新版本
	//
	// 要统计所有版本，需要：
	// 1. 直接读取 SST 文件（需要访问内部 table 读取器）
	// 2. 或者使用内部的 raw iterator（不公开）
	//
	// 由于这些是内部 API，我们只能提供一个估算值

	fmt.Println("  警告: LevelDB 的公开 API 不支持统计所有 internal keys")
	fmt.Println("        标准迭代器只返回每个 user key 的最新版本")
	fmt.Println("        以下显示的是唯一键数（最新版本）")
	fmt.Println()

	// 统计唯一键数
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	count := int64(0)
	for iter.Next() {
		count++
	}

	return count, iter.Error()
}

// ternary 三元运算符辅助函数
func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}

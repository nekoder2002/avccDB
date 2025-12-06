# Merkle树更新

<cite>
**本文引用的文件**
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go)
- [leveldb/merkle/node.go](file://leveldb/merkle/node.go)
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go)
- [leveldb/db.go](file://leveldb/db.go)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go)
- [leveldb/table.go](file://leveldb/table.go)
</cite>

## 目录
1. [引言](#引言)
2. [项目结构](#项目结构)
3. [核心组件](#核心组件)
4. [架构总览](#架构总览)
5. [详细组件分析](#详细组件分析)
6. [依赖关系分析](#依赖关系分析)
7. [性能考量](#性能考量)
8. [故障排查指南](#故障排查指南)
9. [结论](#结论)

## 引言
本文件围绕 avccDB 的 Merkle 树更新流程展开，聚焦于 PutWithVersion 操作后的 Merkle 树更新路径：从内存数据库收集键值对、构建新 Merkle 树、通过 UpdateLeaf 方法实现不可变性维护、压缩操作触发树重算，以及脏标记（dirty flag）对重建时机的控制。同时提供数据流图与性能优化建议（延迟构建、缓存）。

## 项目结构
- Merkle 核心位于 leveldb/merkle，包含节点、树、构建器、序列化等模块。
- 内存数据库 leveldb/memdb 提供带 Merkle 支持的封装，包含脏标记与根哈希缓存。
- 数据库层 leveldb/db 负责 MasterRoot（全库聚合根）的计算与更新，贯穿 flush/compaction 等关键事件。
- 压缩流程 leveldb/db_compaction 在 mem flush 与 table compaction 后调用 MasterRoot 更新。
- 表层 leveldb/table 提供从 SST 获取 Merkle 根的能力，支撑层级聚合。

```mermaid
graph TB
subgraph "内存层"
MDB["MerkleDB<br/>memdb_merkle.go"]
end
subgraph "Merkle树"
MT["MerkleTree<br/>tree.go"]
TB["TreeBuilder<br/>tree_builder.go"]
ND["MerkleNode<br/>node.go"]
end
subgraph "数据库层"
DB["DB<br/>db.go"]
COMP["压缩流程<br/>db_compaction.go"]
TBL["表操作<br/>table.go"]
end
MDB --> MT
MT --> TB
TB --> ND
DB --> TBL
COMP --> DB
DB --> MT
```

图表来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/merkle/node.go](file://leveldb/merkle/node.go#L1-L52)
- [leveldb/db.go](file://leveldb/db.go#L1480-L1571)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/db.go](file://leveldb/db.go#L1480-L1571)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

## 核心组件
- MerkleTree：维护根节点、叶子映射、统计信息；提供获取根、查询、生成证明、校验证明、更新叶子等能力。
- TreeBuilder：自底向上构建平衡二叉 Merkle 树，支持批量添加叶子、排序去重、流式构建等。
- MerkleDB：在内存数据库之上封装 Merkle 能力，维护脏标记与根哈希缓存，提供 BuildMerkleTree 与 GetRootHash 等接口。
- DB：负责 MasterRoot 的计算与更新，贯穿 mem flush 与 table compaction。
- 表层 tOps：从 SST 获取 Merkle 根，支撑层级聚合。

章节来源
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)
- [leveldb/db.go](file://leveldb/db.go#L1480-L1571)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

## 架构总览
Merkle 树更新的关键路径：
- 写入 PutWithVersion：仅写入底层内存数据库，设置脏标记，不立即重建树。
- 读取 GetRootHash/GetWithProof：若脏则触发 BuildMerkleTree，从内存数据库迭代收集键值对，按排序构建树，更新根哈希与脏标记。
- 压缩触发：mem flush 与 table compaction 后调用 updateMasterRoot，基于各层 SST 的 Merkle 根聚合出 MasterRoot。

```mermaid
sequenceDiagram
participant App as "应用"
participant MDB as "MerkleDB<br/>memdb_merkle.go"
participant MT as "MerkleTree<br/>tree.go"
participant TB as "TreeBuilder<br/>tree_builder.go"
participant DB as "DB<br/>db.go"
participant COMP as "压缩流程<br/>db_compaction.go"
App->>MDB : PutWithVersion(key,value,version)
MDB->>MDB : 写入底层DB并置脏标记
App->>MDB : GetRootHash()/GetWithProof()
MDB->>MDB : 若脏则BuildMerkleTree()
MDB->>MT : 迭代底层DB收集KV对
MT->>TB : 使用排序去重与构建器重建树
TB-->>MT : 返回新根
MT-->>MDB : 更新根哈希与索引
MDB-->>App : 返回根哈希/证明
Note over COMP,DB : 压缩完成后
COMP->>DB : memCompaction()/tableCompaction()
DB->>DB : updateMasterRoot()
DB-->>DB : 计算各层SST根并聚合为MasterRoot
```

图表来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L42-L124)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L53-L120)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L82-L131)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/db.go](file://leveldb/db.go#L1562-L1571)

## 详细组件分析

### PutWithVersion 后的树更新
- PutWithVersion 仅写入底层内存数据库，随后将脏标记置为 true，表示树需要重建。
- 下次读取（GetRootHash 或 GetWithProof）时，若发现脏标记为真，则执行 BuildMerkleTree。

```mermaid
flowchart TD
Start(["PutWithVersion"]) --> Write["写入底层DB"]
Write --> Dirty["设置脏标记=true"]
Dirty --> Wait["等待下一次读取"]
Wait --> Check{"是否脏？"}
Check -- 否 --> End(["结束"])
Check -- 是 --> Build["BuildMerkleTree()<br/>迭代底层DB收集KV对"]
Build --> Rebuild["TreeBuilder重建树"]
Rebuild --> Update["更新根哈希与索引"]
Update --> End
```

图表来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L42-L124)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L53-L120)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L82-L131)

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L42-L124)

### BuildMerkleTree：从内存数据库收集并重建
- BuildMerkleTree 在持有写锁时执行，避免并发冲突。
- 通过底层 DB 的迭代器遍历当前状态，收集键值对（版本提取逻辑在注释中提示需从内部键解析）。
- 使用 BuildFromSorted 与 TreeBuilder 构建平衡树，返回 MerkleTree 并缓存根哈希，清除脏标记。

```mermaid
sequenceDiagram
participant MDB as "MerkleDB"
participant DB as "底层DB"
participant TB as "TreeBuilder"
participant MT as "MerkleTree"
MDB->>MDB : 加写锁
MDB->>DB : NewIterator()
MDB->>DB : 遍历并收集KV对
MDB->>TB : BuildFromSorted()
TB-->>MDB : 返回根节点
MDB->>MT : NewMerkleTree(root)
MDB->>MDB : 缓存rootHash并清空脏标记
MDB-->>MDB : 解锁
```

图表来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L58-L106)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L140-L151)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L21-L38)

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L58-L106)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L140-L151)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L21-L38)

### UpdateLeaf：通过重建整棵树维护不可变性
- UpdateLeaf 会收集现有叶子，定位目标键并更新或插入，随后使用 TreeBuilder 重新构建整棵树，保证不可变性。
- 重建后更新根节点、重建叶子索引，返回新根。

```mermaid
flowchart TD
UStart(["UpdateLeaf"]) --> Collect["CollectLeaves(root)"]
Collect --> Find{"找到键？"}
Find -- 是 --> Update["替换为新叶子"]
Find -- 否 --> Insert["新增叶子并排序去重"]
Update --> Rebuild["TreeBuilder.AddLeaf/Build()"]
Insert --> Rebuild
Rebuild --> Index["重建叶子索引"]
Index --> UEnd(["返回新根"])
```

图表来源
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L227-L275)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L45-L80)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L82-L131)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L402-L430)

章节来源
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L227-L275)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L45-L80)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L402-L430)

### 压缩操作后触发树重算与 MasterRoot 更新
- mem flush 与 table compaction 完成后，调用 updateMasterRoot。
- computeMasterRoot 遍历版本层次，从每个 SST 获取 Merkle 根，先按层聚合为 Layer Root，再将所有 Layer Root 聚合为 MasterRoot。
- DB 层提供 GetMasterRoot 接口以供上层使用。

```mermaid
sequenceDiagram
participant COMP as "压缩流程"
participant DB as "DB"
participant V as "版本"
participant TBL as "表操作"
participant MR as "MasterRoot"
COMP->>DB : memCompaction()/tableCompaction()
DB->>DB : updateMasterRoot()
DB->>V : 获取当前版本
loop 遍历各级别
DB->>TBL : 对每张SST调用getMerkleRoot()
TBL-->>DB : 返回SST根
DB->>DB : 层内聚合为Layer Root
end
DB->>DB : 所有Layer Root聚合为MasterRoot
DB-->>MR : 更新缓存
```

图表来源
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L627-L629)
- [leveldb/db.go](file://leveldb/db.go#L1491-L1561)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

章节来源
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L627-L629)
- [leveldb/db.go](file://leveldb/db.go#L1491-L1561)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

### 脏标记（dirty flag）与重建时机
- MerkleDB 维护 dirty 字段与 rootHash 缓存。
- 读取路径（GetRootHash/GetWithProof）在发现脏标记为真时，释放读锁并获取写锁，执行 BuildMerkleTree，完成后恢复读锁。
- 写入路径（PutWithVersion）仅置脏标记，不阻塞写入，从而实现延迟构建。

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L14-L56)
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L108-L148)

### 数据模型与类关系
```mermaid
classDiagram
class MerkleTree {
-root : MerkleNode
-compare : func
-leafMap : map[string]*MerkleNode
-stats : TreeStats
+GetRoot() Hash
+Get(key) (value,version,bool)
+GenerateProof(key) MerkleProof
+VerifyProof(proof) bool
+UpdateLeaf(key,value,version) *MerkleNode
+GetStats() TreeStats
+RootNode() *MerkleNode
}
class TreeBuilder {
-compare : func
-leaves : []*MerkleNode
-nodeStack : []*MerkleNode
-totalNodes : int
-totalLeaves : int
-treeHeight : int
+AddLeaf(key,value,version) error
+AddLeaves(leaves) error
+Build() *MerkleNode
+BuildFromSorted(pairs,compare) *MerkleNode
+GetStats() TreeStats
}
class MerkleNode {
+Hash : Hash
+NodeType : NodeType
+Key : []byte
+Value : []byte
+Version : uint64
+Left : *MerkleNode
+Right : *MerkleNode
+LeftOffset : int64
+RightOffset : int64
+Height : int32
+IsLeaf() bool
+IsInternal() bool
}
class MerkleDB {
-DB : *memdb.DB
-tree : *MerkleTree
-dirty : bool
-rootHash : Hash
+PutWithVersion(key,value,version) error
+BuildMerkleTree() error
+GetRootHash() Hash
+GetWithProof(key) MerkleProof
+GetTree() *MerkleTree
+GetMerkleStats() MerkleStats
}
MerkleTree --> MerkleNode : "包含"
TreeBuilder --> MerkleNode : "构建"
MerkleDB --> MerkleTree : "封装"
```

图表来源
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/merkle/node.go](file://leveldb/merkle/node.go#L1-L52)
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)

## 依赖关系分析
- MerkleDB 依赖 memdb.DB 与 merkle 包；读取时在必要时重建树。
- DB 依赖版本系统与表操作，用于计算 MasterRoot。
- TreeBuilder 依赖 MerkleNode 与排序工具，提供自底向上的平衡树构建。
- 压缩流程在 mem flush 与 table compaction 后更新 MasterRoot。

```mermaid
graph LR
MDB["MerkleDB"] --> MBD["memdb.DB"]
MDB --> MT["MerkleTree"]
MT --> TB["TreeBuilder"]
TB --> ND["MerkleNode"]
DB["DB"] --> V["version"]
DB --> TBL["tOps"]
TBL --> SST["SSTable"]
COMP["压缩流程"] --> DB
DB --> MT
```

图表来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/db.go](file://leveldb/db.go#L1480-L1571)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L1-L181)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L1-L286)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L1-L430)
- [leveldb/db.go](file://leveldb/db.go#L1480-L1571)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

## 性能考量
- 延迟构建：PutWithVersion 不重建树，仅置脏标记；首次读取时才重建，降低写放大。
- 缓存：MerkleDB 缓存 rootHash，避免重复计算；DB 层缓存 MasterRoot，减少聚合开销。
- 自底向上构建：TreeBuilder 使用栈式自底向上构建，内存友好且保持平衡。
- 流式构建：StreamingTreeBuilder 支持分批构建子树并合并，适合大规模数据。
- 排序去重：SortAndDeduplicate 在重建前确保同一用户键保留最高版本，减少冗余。
- 压缩后聚合：mem flush 与 table compaction 后统一更新 MasterRoot，避免频繁小粒度重算。

章节来源
- [leveldb/memdb/memdb_merkle.go](file://leveldb/memdb/memdb_merkle.go#L14-L56)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L190-L295)
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L402-L430)
- [leveldb/db.go](file://leveldb/db.go#L1491-L1561)

## 故障排查指南
- 版本顺序错误：TreeBuilder 在 AddLeaf 时要求键有序且同键高版本优先，违反将报错。检查写入键编码与版本号生成逻辑。
- 空树错误：当底层数据库为空时，BuildMerkleTree 将返回空树状态；确认写入是否成功。
- 证明校验失败：VerifyProof 先比对证明根与当前树根，再验证路径；若失败，检查树是否被重建或版本是否匹配。
- MasterRoot 不更新：确认 mem flush 与 table compaction 是否触发 updateMasterRoot；检查表层是否正确返回 SST 根。

章节来源
- [leveldb/merkle/tree_builder.go](file://leveldb/merkle/tree_builder.go#L45-L80)
- [leveldb/merkle/tree.go](file://leveldb/merkle/tree.go#L213-L225)
- [leveldb/db_compaction.go](file://leveldb/db_compaction.go#L336-L354)
- [leveldb/table.go](file://leveldb/table.go#L444-L478)

## 结论
avccDB 的 Merkle 树更新采用“写延迟、读重建”的策略，结合脏标记与缓存，既保证了写入性能又确保读取一致性。压缩流程后统一更新 MasterRoot，形成从 SST 根到层根再到全库根的三层聚合结构。UpdateLeaf 通过重建整棵树维护不可变性，配合排序去重与流式构建，满足大规模数据场景下的可扩展性与正确性。
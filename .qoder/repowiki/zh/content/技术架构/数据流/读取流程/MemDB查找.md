# MemDB查找

<cite>
**本文引用的文件**
- [leveldb/db.go](file://leveldb/db.go)
- [leveldb/db_state.go](file://leveldb/db_state.go)
- [leveldb/key.go](file://leveldb/key.go)
- [leveldb/memdb/memdb.go](file://leveldb/memdb/memdb.go)
</cite>

## 目录
1. [简介](#简介)
2. [项目结构](#项目结构)
3. [核心组件](#核心组件)
4. [架构总览](#架构总览)
5. [详细组件分析](#详细组件分析)
6. [依赖关系分析](#依赖关系分析)
7. [性能考量](#性能考量)
8. [故障排查指南](#故障排查指南)
9. [结论](#结论)

## 简介
本文件聚焦于 avccDB 的 MemDB 查找流程，系统性梳理从客户端发起 Get 请求到在内存数据库中完成查找的完整路径。重点覆盖：
- 如何通过 getEffectiveMem 和 getFrozenMem 获取当前有效的内存表与冻结的内存表
- db.get 方法如何依次查询辅助 MemDB（auxm）、有效 MemDB、冻结 MemDB
- memGet 函数如何解析内部键并处理版本化键与非版本化键，以及如何基于序列号判断数据可见性
- 在多版本查询场景下，如何收集所有匹配版本的数据
- 提供 MemDB 查找流程的序列图，标注关键数据结构访问与版本匹配逻辑

## 项目结构
围绕 MemDB 查找的关键代码分布在以下模块：
- 数据库主入口与读取流程：leveldb/db.go
- 内存表状态管理（有效/冻结）：leveldb/db_state.go
- 内部键编码/解码与用户键提取：leveldb/key.go
- 内存数据库实现（跳表结构、查找接口）：leveldb/memdb/memdb.go

```mermaid
graph TB
subgraph "数据库层"
DB["DB 结构体<br/>持有 mem/frozenMem 等"]
GET["Get/GetWithVersion<br/>读取入口"]
GETP["GetWithProof<br/>带证明读取"]
GH["GetVersionHistory<br/>版本历史查询"]
end
subgraph "内存表状态"
GETE["getEffectiveMem<br/>获取有效 MemDB"]
GETF["getFrozenMem<br/>获取冻结 MemDB"]
GETMS["getMems<br/>获取有效+冻结"]
end
subgraph "内存数据库"
MEMDB["memdb.DB<br/>跳表实现"]
FIND["Find/Get<br/>查找接口"]
end
subgraph "内部键处理"
MK["makeInternalKeyWithVersion<br/>构造内部键"]
PARS["parseInternalKey/WithVersion<br/>解析内部键"]
UKEY["internalKey.ukey()<br/>提取用户键"]
end
DB --> GET
DB --> GETP
DB --> GH
GET --> GETMS
GETP --> GETMS
GH --> GETMS
GETMS --> GETE
GETMS --> GETF
GETE --> MEMDB
GETF --> MEMDB
GET --> MK
GETP --> MK
GH --> MK
MEMDB --> FIND
MK --> PARS
PARS --> UKEY
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L798-L968)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L197)
- [leveldb/memdb/memdb.go](file://leveldb/memdb/memdb.go#L351-L386)

章节来源
- [leveldb/db.go](file://leveldb/db.go#L798-L968)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L197)
- [leveldb/memdb/memdb.go](file://leveldb/memdb/memdb.go#L351-L386)

## 核心组件
- DB.get：统一的读取入口，负责构建内部键、按顺序查询 auxm、有效 MemDB、冻结 MemDB，并回退到版本层（SST 文件）。
- memGet：在单个 MemDB 中执行查找，解析内部键，区分版本化与非版本化键，依据类型判定删除或返回值。
- getEffectiveMem/getFrozenMem/getMems：线程安全地获取有效/冻结内存表指针，用于并发读取。
- makeInternalKeyWithVersion/parseInternalKey/parseInternalKeyWithVersion/internalKey.ukey：内部键编码与解析，支持带版本的键格式。

章节来源
- [leveldb/db.go](file://leveldb/db.go#L769-L833)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L197)

## 架构总览
MemDB 查找遵循“先 auxm，再有效 MemDB，再冻结 MemDB”的顺序；每个层级均使用内部键进行查找，内部键包含用户键、版本号、序列号与类型字段。版本化键优先匹配，若未命中则尝试非版本化键；删除类型键直接视为不存在。

```mermaid
sequenceDiagram
participant Client as "客户端"
participant DB as "DB"
participant State as "内存表状态"
participant Aux as "auxm"
participant Eff as "有效 MemDB"
participant Fro as "冻结 MemDB"
participant Mem as "memGet"
participant IK as "内部键工具"
Client->>DB : 调用 Get/GetWithVersion
DB->>IK : 构造内部键(含版本/序列/类型)
DB->>State : getMems() 获取有效+冻结
State-->>DB : 返回有效/冻结 MemDB 指针
DB->>Aux : memGet(ikey)
alt 命中
Aux-->>DB : 返回值/错误
DB-->>Client : 返回结果
else 未命中
DB->>Eff : memGet(ikey)
alt 命中
Eff-->>DB : 返回值/错误
DB-->>Client : 返回结果
else 未命中
DB->>Fro : memGet(ikey)
alt 命中
Fro-->>DB : 返回值/错误
DB-->>Client : 返回结果
else 未命中
DB->>DB : 回退到版本层(SST)
DB-->>Client : 返回结果或错误
end
end
end
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L798-L833)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L104)

## 详细组件分析

### 组件A：getEffectiveMem 与 getFrozenMem
- getEffectiveMem：读锁保护下返回当前有效 MemDB 的引用计数加一；若 DB 未关闭但有效 MemDB 为空，则抛出异常。
- getFrozenMem：读锁保护下返回冻结 MemDB 的引用计数加一；若存在则增加引用，否则返回空。
- getMems：同时返回有效与冻结 MemDB 的引用计数加一，便于后续顺序遍历。

```mermaid
flowchart TD
Start(["进入 getEffectiveMem/getFrozenMem"]) --> Lock["读锁获取"]
Lock --> CheckNil{"mem/frozen 是否为空？"}
CheckNil --> |有效| IncRef["引用计数+1"]
CheckNil --> |无效且未关闭| Panic["抛出异常"]
CheckNil --> |无效且已关闭| ReturnNil["返回 nil"]
IncRef --> Unlock["释放读锁"]
ReturnNil --> Unlock
Unlock --> End(["返回指针"])
```

图表来源
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)

章节来源
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)

### 组件B：db.get 的查找顺序与回退
- 构造内部键：当 version=0 时使用特殊标记以匹配任意版本；随后将用户键、版本号、序列号与类型组合为内部键。
- 查询顺序：auxm -> 有效 MemDB -> 冻结 MemDB。
- 若所有层级均未命中，则回退到版本层（SST 文件），并在需要时触发表压缩。

```mermaid
flowchart TD
S(["开始"]) --> BuildIK["构造内部键(含版本/序列/类型)"]
BuildIK --> TryAux{"auxm 存在？"}
TryAux --> |是| CheckAux["memGet(auxm,ikey)"]
TryAux --> |否| TryEff["获取有效 MemDB"]
CheckAux --> FoundAux{"命中？"}
FoundAux --> |是| ReturnAux["返回值/错误"]
FoundAux --> |否| TryEff
TryEff --> CheckEff["memGet(effective,ikey)"]
CheckEff --> FoundEff{"命中？"}
FoundEff --> |是| ReturnEff["返回值/错误"]
FoundEff --> |否| TryFro["获取冻结 MemDB"]
TryFro --> CheckFro["memGet(frozen,ikey)"]
CheckFro --> FoundFro{"命中？"}
FoundFro --> |是| ReturnFro["返回值/错误"]
FoundFro --> |否| SST["回退到版本层(SST)并可触发压缩"]
SST --> End(["结束"])
ReturnAux --> End
ReturnEff --> End
ReturnFro --> End
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L798-L833)

章节来源
- [leveldb/db.go](file://leveldb/db.go#L798-L833)

### 组件C：memGet 的内部键解析与可见性判断
- 使用 MemDB.Find 定位首个大于等于目标内部键的条目。
- 优先尝试解析为版本化内部键（包含版本号字段），比较用户键一致后：
  - 类型为删除则视为未找到；
  - 否则返回该值。
- 若解析失败，尝试解析为非版本化内部键（不含版本号字段），同样比较用户键一致后：
  - 类型为删除则视为未找到；
  - 否则返回该值。
- 其他错误（非“未找到”）直接返回。

```mermaid
flowchart TD
Enter(["进入 memGet"]) --> Find["mdb.Find(ikey)"]
Find --> ErrNF{"错误=未找到？"}
ErrNF --> |是| ReturnMiss["返回未命中"]
ErrNF --> |否| ParseVer["尝试解析为版本化内部键"]
ParseVer --> VerOK{"解析成功？"}
VerOK --> |是| CmpUVer["比较用户键是否一致"]
CmpUVer --> |否| ParseNonVer["尝试解析为非版本化内部键"]
CmpUVer --> |是| TypeVer{"类型=删除？"}
TypeVer --> |是| ReturnNF["返回未找到"]
TypeVer --> |否| ReturnVal["返回值"]
ParseNonVer --> NonOK{"解析成功？"}
NonOK --> |是| CmpUNon["比较用户键是否一致"]
CmpUNon --> |否| ReturnMiss
CmpUNon --> |是| TypeNon{"类型=删除？"}
TypeNon --> |是| ReturnNF
TypeNon --> |否| ReturnVal
NonOK --> |否| Panic["异常(不可能)"]
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L769-L796)
- [leveldb/key.go](file://leveldb/key.go#L106-L146)

章节来源
- [leveldb/db.go](file://leveldb/db.go#L769-L796)
- [leveldb/key.go](file://leveldb/key.go#L106-L146)

### 组件D：内部键格式与用户键提取
- 版本化内部键格式：用户键 + 版本号(8字节) + 序列号+类型(8字节)
- 非版本化内部键格式：用户键 + 序列号+类型(8字节)
- 解析函数：
  - parseInternalKeyWithVersion：解析版本化键，提取用户键、版本号、序列号与类型
  - parseInternalKey：解析非版本化键
  - internalKey.ukey：根据键长度判断是否包含版本字段，提取用户键
- hasVersion/extractVersion：辅助判断与提取版本号

```mermaid
classDiagram
class InternalKey {
+ukey() []byte
+num() uint64
+parseNum() (seq, kt)
+String() string
}
class KeyUtil {
+makeInternalKeyWithVersion(dst, ukey, version, seq, kt) internalKey
+parseInternalKey(ik) (ukey, seq, kt)
+parseInternalKeyWithVersion(ik) (ukey, version, seq, kt)
+hasVersion(ik) bool
+extractVersion(ik) (version, ok)
}
InternalKey <.. KeyUtil : "被使用"
```

图表来源
- [leveldb/key.go](file://leveldb/key.go#L84-L197)

章节来源
- [leveldb/key.go](file://leveldb/key.go#L84-L197)

### 组件E：多版本查询与版本历史收集
- 多版本查询：db.getVersionHistory 将 auxm、有效/冻结 MemDB 与版本层（SST）的结果合并，去重并按版本升序返回。
- MemDB 版本收集：collectVersionsFromMemDB 使用迭代器从 MemDB 中收集满足范围的版本，跳过删除项；仅保留 MemDB 的优先级值。
- 版本边界：minVersion=0 表示无下界，maxVersion=0 表示无上界。

```mermaid
flowchart TD
StartV(["开始版本历史查询"]) --> InitMap["初始化版本映射"]
InitMap --> ScanAux{"auxm 存在？"}
ScanAux --> |是| CollectAux["collectVersionsFromMemDB(auxm)"]
ScanAux --> |否| ScanEff["获取有效 MemDB"]
CollectAux --> ScanEff
ScanEff --> CollectEff["collectVersionsFromMemDB(effective)"]
CollectEff --> ScanFro["获取冻结 MemDB"]
ScanFro --> CollectFro["collectVersionsFromMemDB(frozen)"]
CollectFro --> ScanSST["版本层(SST)获取版本历史"]
ScanSST --> Merge["合并SST与MemDB结果(去重/优先级)"]
Merge --> Sort["按版本升序排序"]
Sort --> ReturnV(["返回版本列表"])
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L934-L1047)
- [leveldb/db.go](file://leveldb/db.go#L995-L1047)

章节来源
- [leveldb/db.go](file://leveldb/db.go#L934-L1047)
- [leveldb/db.go](file://leveldb/db.go#L995-L1047)

## 依赖关系分析
- DB.get 依赖：
  - getMems 获取有效/冻结 MemDB
  - memGet 在单个 MemDB 中执行查找
  - 内部键工具：makeInternalKeyWithVersion、parseInternalKey/WithVersion、internalKey.ukey
- memGet 依赖：
  - memdb.DB.Find 接口
  - 内部键解析工具
- 内部键工具独立于 DB 层，提供通用的键格式处理能力。

```mermaid
graph LR
DB_get["DB.get"] --> getMems["getMems"]
DB_get --> memGet["memGet"]
memGet --> memdb_find["memdb.DB.Find"]
DB_get --> mkIK["makeInternalKeyWithVersion"]
memGet --> parseIK["parseInternalKey/WithVersion"]
parseIK --> ukey["internalKey.ukey"]
```

图表来源
- [leveldb/db.go](file://leveldb/db.go#L769-L833)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L197)
- [leveldb/memdb/memdb.go](file://leveldb/memdb/memdb.go#L351-L386)

章节来源
- [leveldb/db.go](file://leveldb/db.go#L769-L833)
- [leveldb/db_state.go](file://leveldb/db_state.go#L161-L211)
- [leveldb/key.go](file://leveldb/key.go#L84-L197)
- [leveldb/memdb/memdb.go](file://leveldb/memdb/memdb.go#L351-L386)

## 性能考量
- 并发安全：getEffectiveMem/getFrozenMem/getMems 使用读写锁保护，避免竞态；返回的 MemDB 指针需在使用后调用 decref 归还引用。
- 查找复杂度：memdb.DB 采用跳表结构，查找近似 O(log n)，Find/Get 均为 O(log n)。
- 内部键开销：版本化键比非版本化键多 8 字节版本号，但带来的查询灵活性与历史追踪收益通常更大。
- 迭代器扫描：版本历史查询在 MemDB 中使用 NewIterator 并 Seek 到目标键，随后线性遍历，注意在大表上可能带来额外成本。

## 故障排查指南
- 未找到（ErrNotFound）：
  - 可能原因：目标键不存在、类型为删除、内部键解析失败
  - 排查要点：确认内部键构造参数（version、seq、kt）正确；检查用户键比较器是否一致
- 引用泄漏：
  - 现象：getMems 返回的 MemDB 指针未调用 decref 导致资源无法回收
  - 排查要点：确保每次使用后调用 decref
- 键格式错误：
  - 现象：parseInternalKey/WithVersion 抛出“长度不足/类型非法”
  - 排查要点：确认内部键生成函数与解析函数一致；检查序列号范围与类型枚举

章节来源
- [leveldb/db.go](file://leveldb/db.go#L769-L833)
- [leveldb/key.go](file://leveldb/key.go#L106-L146)

## 结论
MemDB 查找流程通过严格的内部键格式与解析机制，实现了对版本化与非版本化键的统一处理，并在多层级（auxm、有效 MemDB、冻结 MemDB）间有序回退。getEffectiveMem 与 getFrozenMem 提供了线程安全的内存表访问；memGet 则在单个 MemDB 内完成精确匹配与可见性判断。配合版本历史查询，系统能够满足溯源与多版本一致性需求。
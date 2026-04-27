# openGemini TSSP Compaction Spec

本文档是 TSSP compaction 的细粒度函数级规范，对应 `tssp_compact_design.md` 中的设计说明。

## 1. 常量与配置

### 1.1 Compact 常量

```go
const (
    CompactLevels    = 7      // 层级数量：0-6
    minFileSizeLimit = 1 * 1024 * 1024  // 最小文件大小限制
)
```

### 1.2 压缩规则

```go
// LevelCompactRule 定义了层级压缩的执行顺序
// 每次 Compact 调用按此顺序遍历各层级
LevelCompactRule = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}

// ColumnStore 的压缩规则（目前只做 level 0 和 level 1）
LevelCompactRuleForCs = []uint16{0, 1, 0, 1, 0, 1}

// 每层最少的组文件数
LeveLMinGroupFiles = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
// Level:                           {0,  1,  2,  3,  4,  5,  6}
```

### 1.3 并发控制

```go
var (
    maxFullCompactor = cpu.GetCpuNum() / 2  // Full compact 最大并发
    maxCompactor = cpu.GetCpuNum()            // Level compact 最大并发
    compLimiter = limiter.NewFixed(maxCompactor)
)
```

## 2. 核心数据结构

### 2.1 CompactGroup

```go
type CompactGroup struct {
    name      string      // measurement 名称（含版本）
    toLevel   uint16      // 目标层级
    group     []string    // 参与压缩的文件路径列表
    shardId   uint64      // shard ID
    dropping  *int64      // 指向 measurement closing 状态
}
```

### 2.2 CompactTask

```go
type CompactTask struct {
    plan  *CompactGroup
    table *MmsTables
    full  bool  // 是否为 FullCompact
}
```

### 2.3 FilesInfo

```go
type FilesInfo struct {
    name         string
    toLevel      uint16
    oldFiles     []TSSPFile
    oldFids      []string
    compIts      FileIterators
    maxChunkN    int
    estimateSize int
    maxColumns   int
    maxChunkRows int
    avgChunkRows int64
    dropping     *int64
    shId         uint64
}
```

## 3. 函数规格

### 3.1 shard.Compact()

```go
func (s *shard) Compact() error
```

**前置条件：**
- shard 未关闭 (`!s.closed.Signal()`)

**处理流程：**
1. 如果是下采样 shard，直接返回 nil
2. 根据 `engineType` 选择压缩规则
3. 检查 compaction 是否启用，未启用则返回
4. 获取当前时间和上次写入时间
5. 如果 `nowTime - lastWrite >= fullCompColdDuration`，执行 `FullCompact`
6. 否则按 `LevelCompactRule` 顺序执行 `LevelCompact`

**异常处理：**
- 每个层级的错误记录但继续其他层级

---

### 3.2 MmsTables.LevelCompact()

```go
func (m *MmsTables) LevelCompact(level uint16, shid uint64) error
```

**前置条件：**
- `level < CompactLevels`
- `level < MaxCompactionLevel` (如果配置了)

**处理流程：**
1. 调用 `ImmTable.LevelPlan(m, level)` 获取压缩计划
2. 如果计划为空，直接返回
3. 调用 `buildCompactTaskGroup` 构建任务组
4. 通过 scheduler 执行任务组

**异常处理：**
- 返回第一个非 nil 错误

---

### 3.3 MmsTables.FullCompact()

```go
func (m *MmsTables) FullCompact(shid uint64) error
```

**前置条件：**
- `maxFullCompactor - atomic.LoadInt64(&fullCompactingCount) >= 1`

**处理流程：**
1. 计算可用 full compactor 数量
2. 如果 `PreFullCompactLevel > 0`，先执行 pre-level full compact
3. 构建 full compact 计划
4. 批量执行任务

**并发控制：**
- 通过 `fullCompactingCount` 原子计数限制

---

### 3.4 tsImmTableImpl.LevelPlan()

```go
func (t *tsImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup
```

**前置条件：**
- `m.CompactionEnabled() == true`

**处理流程：**
1. 获取该层的 `minGroupFileN = LeveLMinGroupFiles[level]`
2. 遍历 `m.Order` 中的所有 measurement
3. 对每个 measurement 调用 `getMmsPlan`

**注意：**
- 只处理 Order 文件，不处理 OutOfOrder
- OutOfOrder 通过 `MergeOutOfOrder` 处理

---

### 3.5 MmsTables.mmsPlan()

```go
func (m *MmsTables) mmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup
```

**前置条件：**
- `files.closing == 0`
- `m` 未关闭
- `m.stopCompMerge` 未关闭

**处理流程：**
1. 使用 `seqMap` 按 sequence 分组文件
2. 遍历文件列表：
   - 如果文件层级不等于目标 level，生成当前组计划并重置
   - 否则按 sequence 分组
3. 最后生成未完成的组计划

**关键逻辑：**
```go
// 同一 sequence 的文件必须一起压缩
// 因为它们共享相同的 sequence 编号
seqByte := record.Uint64ToBytesUnsafe(seq)
if !seqMap.HasBytes(seqByte) {
    plans = m.genCompactPlan(...)
    seqMap.SetBytes(seqByte, f)
}
```

---

### 3.6 MmsTables.genCompactGroup()

```go
func (m *MmsTables) genCompactGroup(seqMap *dictpool.Dict, name string, level uint16) *CompactGroup
```

**前置条件：**
- `seqMap.Len() >= minGroupFileN`
- 该 measurement 未被其他 compaction 占用
- 文件未处于 parquet 处理中

**处理流程：**
1. 如果是 BLOCK compaction 类型，检查 `inBlockCompact`
2. 创建 `CompactGroup`，level = `seqMap.Len()`
3. 检查是否繁忙或正在 parquet 处理

---

### 3.7 CompactTask.Execute()

```go
func (t *CompactTask) Execute()
```

**前置条件：**
- `BeforeExecute() == true` (获取到文件锁)

**处理流程：**
1. 如果 `group.Len() == 1`，直接 rename 到目标层级
2. 否则：
   a. 调用 `refMmsTable` 增加文件引用
   b. 调用 `NewFileIterators` 创建迭代器
   c. 调用 `compactToLevel` 执行压缩
   d. 如果是 full compact 且配置了 parquet，标记 parquet 完成
3. 释放文件引用

**异常恢复：**
- 如果 `CompactRecovery` 启用，panic 被捕获并记录

---

### 3.8 tsImmTableImpl.compactToLevel()

```go
func (t *tsImmTableImpl) compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error
```

**前置条件：**
- `len(group.oldFiles) >= 2` (或 == 1 用于 rename)

**处理流程：**
1. 创建 `CompactStatItem` 统计信息
2. 如果 `isNonStream == true`：
   - 创建 `ChunkIterators`
   - 调用 `m.compact()` 执行非流式压缩
3. 否则：
   - 创建 `StreamIterators`
   - 调用 `compItrs.compact()` 执行流式压缩
4. 替换文件：`m.ReplaceFiles(group.name, group.oldFiles, newFiles, true)`
5. 如果是非流式压缩，添加到热文件管理

---

### 3.9 MmsTables.compact()

```go
func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *zap.Logger) ([]TSSPFile, error)
```

**前置条件：**
- `len(files) >= 1`
- `len(itrs.compIts) >= 2`

**处理流程：**
1. 创建 `MsBuilder` 用于构建新文件
2. 从迭代器循环读取数据：
   ```go
   for {
       id, rec, err := itrs.Next()
       if rec == nil || id == 0 {
           break
       }
       // 如果启用时间乱序纠正，对 record 排序
       if correctTimeDisorder {
           rec = record.SortRecordIfNeeded(rec)
       }
       // 写入 builder
       tableBuilder, err = tableBuilder.WriteRecord(id, rec, ...)
   }
   ```
3. 如果 builder 有数据，创建新的 TSSP 文件
4. 返回新文件列表

---

### 3.10 MmsTables.ReplaceFiles()

```go
func (m *MmsTables) ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool) error
```

**前置条件：**
- `len(newFiles) > 0 && len(oldFiles) > 0`

**处理流程：**
1. 写 compact log (`writeCompactedFileInfo`)
2. Rename tmp 文件 (`RenameTmpFiles`)
3. 从文件集合删除旧文件
4. 添加新文件到集合
5. 排序文件列表
6. 删除 compact log

**异常恢复：**
- 如果失败，根据 compact log 恢复

---

### 3.11 NonStreamingCompaction()

```go
func NonStreamingCompaction(fi FilesInfo) bool
```

**返回值：**
- `true` - 非流式压缩（一次性加载到内存）
- `false` - 流式压缩（按 segment 处理）

**判断逻辑：**
1. 如果 `CorrectTimeDisorder == true`，返回 true
2. 如果 `MergeFlag == NonStreamingCompact`，返回 true
3. 如果 `MergeFlag == StreamingCompact`，返回 false
4. 根据内存占用判断：
   - `avgChunkRows * maxColumns * 8 * len(compIts) >= 128MB` → false
   - `maxChunkRows > maxRowsPerSegment * 500` → false
   - 否则 → true

## 4. 状态机

### 4.1 Measurement 级别并发控制

```
                    acquire()
                        │
                        ▼
                  ┌───────────┐
                  │  inCompact│
                  │   ?       │
                  └─────┬─────┘
                        │
           ┌────────────┴────────────┐
           │                         │
          yes                        no
           │                         │
           ▼                         ▼
      [正在压缩]                  [可以压缩]
           │                         │
           │                    compact 执行
           │                         │
           └──────────┬──────────────┘
                      │
                 CompactDone()
```

### 4.2 Full Compact 流程

```
FullCompact(shid)
      │
      ▼
n = maxFullCompactor - fullCompactingCount
      │
  n < 1? ───yes──→ [返回]
      │
      no
      │
      ▼
PreFullCompactLevel > 0?
      │
  ├──yes──→ buildFullCompactPlan(n, preLevel)
  │              │
  │              ▼
  │         ExecuteBatch(tasks, full=true)
  │              │
  │              ▼
  └──no──→ buildFullCompactPlan(n, 0)
               │
               ▼
          ExecuteBatch(tasks, full=true)
```

## 5. 错误处理

| 错误 | 来源 | 处理 |
| --- | --- | --- |
| `ErrCompStopped` | `m.closed` 或 `m.stopCompMerge` 已关闭 | 停止 compaction |
| `ErrDroppingMst` | measurement 正在删除 | 停止 compaction |
| `ErrFileClosed` | 文件在操作前已关闭 | 跳过该文件 |

## 6. 统计信息

```go
type CompactStatItem struct {
    Full                  bool
    Level                 uint16
    OriginalFileCount     int64
    CompactedFileCount    int64
    OriginalFileSize      int64
    CompactedFileSize    int64
}
```

统计通过 `statistics.CompactStatistics` 收集和推送。

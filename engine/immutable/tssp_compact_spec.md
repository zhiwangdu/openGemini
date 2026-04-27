# openGemini TSSTORE TSSP Compaction Spec

本文档是 TSSTORE TSSP compaction 的函数级规范，对应 `tssp_compact_design.md`。本文只描述 TSSTORE ordered TSSP 文件的 compaction，不展开 Parquet 转换和乱序合并实现。

## 1. 常量与配置

### 1.1 常量

```go
const (
    CompactLevels    = 7
    minFileSizeLimit = 1 * 1024 * 1024
)
```

TSSTORE level 范围为 0 到 6。

### 1.2 Level 规则

```go
LevelCompactRule = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
LeveLMinGroupFiles = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
```

`LevelCompactRule` 是 `shard.Compact` 在热 shard 上遍历 level 的顺序。`LeveLMinGroupFiles[level]` 是生成 level compact plan 的最小唯一 sequence 数。

### 1.3 并发参数

```go
maxCompactor     = cpu.GetCpuNum()
maxFullCompactor = cpu.GetCpuNum() / 2
compLimiter      = limiter.NewFixed(maxCompactor)
```

配置 setter 会修正范围：

- `SetMaxCompactor`: 最小 2，最大 32，0 表示 CPU 数。
- `SetMaxFullCompactor`: 最小 1，最大 32，0 表示 CPU/2；若大于等于 `maxCompactor`，会调整为 `maxCompactor / 2`。

## 2. 数据结构

### 2.1 CompactGroup

`CompactGroup` 描述一次 compact 的文件组：

```go
type CompactGroup struct {
    name     string
    toLevel  uint16
    group    []string
    shardId  uint64
    dropping *int64
}
```

语义：

- `name`: measurement 名称。
- `toLevel`: 新文件目标 level。
- `group`: 参与 compact 的旧文件路径。
- `shardId`: 统计和日志使用的 shard id。
- `dropping`: 指向 `TSSPFiles.closing`，用于任务中止检测。

### 2.2 CompactTask

```go
type CompactTask struct {
    scheduler.BaseTask
    plan  *CompactGroup
    table *MmsTables
    full  bool
}
```

`full` 只表示任务来自 FullCompact 流程；计数和统计会据此标记。

### 2.3 FilesInfo

`FilesInfo` 是由 `NewFileIterators` 构造的执行输入：

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

`NonStreamingCompaction` 使用 `avgChunkRows`、`maxColumns`、`maxChunkRows`、`len(compIts)` 判断是否走流式 compact。

## 3. 函数规格

### 3.1 shard.Compact

```go
func (s *shard) Compact() error
```

流程：

1. downsample shard 直接返回 nil。
2. 如果 shard 已关闭，记录日志并返回 nil。
3. TSSTORE 使用 `immutable.LevelCompactRule`。
4. 如果 `s.immTables.CompactionEnabled()` 为 false，返回 nil。
5. 计算 `fasttime.UnixTimestamp() - s.LastWriteTime()`。
6. 若大于等于 `fullCompColdDuration`，调用 `s.immTables.FullCompact(id)`，记录错误但返回 nil。
7. 否则按 `LevelCompactRule` 调用 `LevelCompact(level, id)`；单个 level 出错只记录并继续。

返回语义：

- 当前实现基本返回 nil；FullCompact/LevelCompact 错误在 shard 层记录日志。

### 3.2 MmsTables.LevelCompact

```go
func (m *MmsTables) LevelCompact(level uint16, shid uint64) error
```

流程：

1. 读取 `config.GetStoreConfig().Compact.MaxCompactionLevel`。
2. 若配置值大于 0 且 `int(level) >= MaxCompactionLevel`，返回 nil。
3. 调用 `m.ImmTable.LevelPlan(m, level)`。
4. 若 plans 为空，返回 nil。
5. 调用 `buildCompactTaskGroup(plans, false, shid)`。
6. 对每个 task group 调用 `m.scheduler.ExecuteTaskGroup(group, m.stopCompMerge)`。
7. 返回 nil。

注意：

- `LevelCompact` 不返回 scheduler 中任务执行错误。
- `level` 必须来自合法规则；函数本身不显式检查 `level < CompactLevels`，越界会在 `LevelPlan` 访问 `LeveLMinGroupFiles[level]` 时出问题。

### 3.3 MmsTables.FullCompact

```go
func (m *MmsTables) FullCompact(shid uint64) error
```

流程：

1. 计算 `n = int64(maxFullCompactor) - atomic.LoadInt64(&fullCompactingCount)`。
2. 若 `n < 1`，返回 nil。
3. 如果 `config.PreFullCompactLevel() > 0`：
   - 调用 `buildFullCompactPlan(n, preLevel)`。
   - 若有 plan，执行 batch 并返回 nil。
4. 调用 `buildFullCompactPlan(n, 0)`。
5. 若有 plan，执行 batch。
6. 返回 nil。

并发语义：

- `fullCompactingCount` 在 `CompactTask.Execute` 内对 `full == true` 的多文件任务增减。
- 单文件 rename 任务不会增减 full compact 计数。

### 3.4 tsImmTableImpl.LevelPlan

```go
func (t *tsImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup
```

流程：

1. 若 `!m.CompactionEnabled()`，返回 nil。
2. 读取 `minGroupFileN := LeveLMinGroupFiles[level]`。
3. 加 `m.mu.RLock()`。
4. 遍历 `m.Order`。
5. 若 `m.isClosed()` 或 `m.isCompMergeStopped()`，停止遍历。
6. 对每个 measurement 调用 `m.getMmsPlan(name, files, level, minGroupFileN, plans)`。
7. 返回 plans。

范围：

- 只处理 ordered 文件集合 `m.Order`。

### 3.5 MmsTables.getMmsPlan

```go
func (m *MmsTables) getMmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup
```

流程：

1. 如果存在 unloaded file，调用 `ReloadSpecifiedFiles`。
2. 加 `files.lock.RLock()`。
3. 若 `files.closing > 0` 或 `files.Len() < minGroupFileN`，返回原 plans。
4. 调用 `m.mmsPlan(...)`。

### 3.6 MmsTables.mmsPlan

```go
func (m *MmsTables) mmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup
```

流程：

1. 若 `m` 已关闭、停止 compact/merge，或 `files.closing > 0`，返回原 plans。
2. 从 `seqMapPool` 获取并 reset `seqMap`。
3. 顺序扫描 `files.files`。
4. 当文件 level 不是目标 level：
   - 调用 `genCompactPlan` 尝试 flush 当前组。
   - reset `seqMap`。
   - `idx++`。
5. 当文件 level 是目标 level 且 sequence 尚未在当前组：
   - 调用 `genCompactPlan` 尝试在加入新 sequence 前 flush 已满组。
   - 如果 `files.splitByUnloadFile(idx)`，reset 当前组。
   - 将当前 sequence/file 加入 `seqMap`。
   - `idx++`。
6. 当遇到当前组里已经存在的 sequence：
   - 跳过后续所有相同 `(level, sequence)` 的文件。
   - reset `seqMap`。
7. 扫描结束后再调用一次 `genCompactPlan`。

关键约束：

- 普通 level compact 组按唯一 sequence 计数。
- 同一 `(level, sequence)` 的多个文件不会在本轮作为一个普通 compact 组一起压缩；实现会跳过该 sequence run 并重置当前组。

### 3.7 MmsTables.genCompactPlan / genCompactGroup

```go
func (m *MmsTables) genCompactPlan(seqMap *dictpool.Dict, minGroupFileN int, name string, level uint16,
    files *TSSPFiles, plans []*CompactGroup) []*CompactGroup

func (m *MmsTables) genCompactGroup(seqMap *dictpool.Dict, name string, level uint16) *CompactGroup
```

`genCompactPlan`：

1. 若 `seqMap.Len() < minGroupFileN`，不生成 plan。
2. 否则调用 `genCompactGroup`。
3. 若 group 非 nil，设置 `plan.dropping = &files.closing` 并 append。
4. reset `seqMap`。

`genCompactGroup`：

1. 创建 `NewCompactGroup(name, level+1, seqMap.Len())`。
2. 将 `seqMap` 中的文件路径填入 `group.group`。
3. 若 `m.busy(group.group)` 或 `InParquetProcess(group.group...)`，release group 并返回 nil。
4. 返回 group。

TSSTORE 的 `tsImmTableImpl.GetCompactionType` 返回 `config.ROW`，因此这里不展开其他 compaction type 的分支。

目标 level：

- 对 level compact，`toLevel = level + 1`。

### 3.8 MmsTables.buildFullCompactPlan

```go
func (m *MmsTables) buildFullCompactPlan(n int64, toLevel uint16) []*CompactGroup
```

流程：

1. 若 compaction 未启用，返回 nil。
2. 初始化 `CompactGroupBuilder`：
   - `limit = int(n)`
   - `parquetLevel = config.TSSPToParquetLevel()`
   - `lowLevelMode = toLevel > 0`
   - `level = toLevel`
3. 遍历 TSSTORE ordered files。
4. 跳过 scheduler 正在运行、measurement closing、已经 full compacted、有 unloaded file 的 measurement。
5. 对每个文件调用 `builder.AddFile(f)`。
6. 普通模式下目标 level 由 `group.UpdateLevel(lv + 1)` 推进。
7. low-level mode 下目标 level 固定为传入 `toLevel`，只收集低于该 level 的文件。
8. 达到 builder limit 后停止。

### 3.9 CompactTask.BeforeExecute

```go
func (t *CompactTask) BeforeExecute() bool
```

流程：

1. 调用 `t.table.acquire(t.plan.group)`。
2. 如果 acquire 失败，返回 false。
3. 注册 finish callback：
   - `CompactDone(t.plan.group)`
   - `blockCompactStop(t.plan.name)`
4. 返回 true。

`acquire` 以文件路径为 key 写入 `m.inCompact`。

### 3.10 CompactTask.Execute

```go
func (t *CompactTask) Execute()
```

流程：

1. 若 `group.Len() == 1`：
   - 调用 `m.RenameFileToLevel(group)`。
   - 记录错误后返回。
2. 对 full compact 调用 `t.IncrFull(1)`。
3. 调用 `m.ImmTable.refMmsTable(m, group.name, false)` 增加文件引用。
4. defer:
   - 如果 `CompactRecovery` 开启，调用 `CompactRecovery(m.path, group)`。
   - unref 文件引用。
   - 对 full compact 调用 `t.IncrFull(-1)`。
5. 若 compaction 已禁用，返回。
6. 调用 `m.ImmTable.NewFileIterators(m, group)`。
7. 调用 `m.ImmTable.compactToLevel(m, fi, t.full, NonStreamingCompaction(fi))`。
8. 如果任务来自 full compact 且配置了 TSSP-to-Parquet level，调用 `markParquetTaskDone`。

注意：

- `CompactTask.Execute` 自身没有 recover panic；它只在 defer 中触发 recovery 检查。
- 单文件 rename 路径不执行 ref/unref，也不增加 full compact count。

### 3.11 MmsTables.RenameFileToLevel

```go
func (m *MmsTables) RenameFileToLevel(plan *CompactGroup) error
```

流程：

1. 根据 plan 路径取出 ordered 文件。
2. 只处理第一个文件。
3. 修改文件名中的 level 为 `plan.toLevel`。
4. 调用 `file.Rename(...)`。
5. rename 成功后调用 `file.UpdateLevel(plan.toLevel)`。

### 3.12 tsImmTableImpl.compactToLevel

```go
func (t *tsImmTableImpl) compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error
```

流程：

1. 创建 `CompactStatItem`，设置 `Full` 和 `Level = group.toLevel - 1`。
2. 根据 `isNonStream` 创建 operation logger。
3. 非流式：
   - `m.NewChunkIterators(group)`。
   - `m.compact(compItrs, group.oldFiles, group.toLevel, true, lcLog)`。
   - close iterators。
4. 流式：
   - `m.NewStreamIterators(group)`。
   - `events = compItrs.InitEvents(group.toLevel)`。
   - `compItrs.compact(group.oldFiles, group.toLevel, true)`。
   - 失败时 `RemoveTmpFiles()`。
   - close iterators。
5. compact 失败则返回错误。
6. 流式路径触发 `events.TriggerReplaceFile(...)`。
7. 调用 `m.ReplaceFiles(group.name, group.oldFiles, newFiles, true)`。
8. 流式路径成功后 `NewHotFileManager().AddAll(newFiles)`。
9. 更新统计项。

### 3.13 MmsTables.compact

```go
func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *logger.Logger) ([]TSSPFile, error)
```

流程：

1. 从第一个输入文件读取 sequence。
2. 构造目标 `TSSPFileName(seq, level, merge=0, extent=0, isOrder, lock)`。
3. 创建 `MsBuilder`。
4. 循环：
   - 若 `m.closed` 或 `m.stopCompMerge` 关闭，返回 `ErrCompStopped`。
   - 调用 `itrs.Next()` 读取下一个 series id 和 record。
   - 若 `rec == nil || id == 0`，结束循环。
   - `record.CheckRecord(rec)`。
   - 若 `CorrectTimeDisorder`，调用 `record.SortRecordIfNeeded(rec)`。
   - 否则调用 `record.CheckTimes(rec.Times())`。
   - 若 `m.indexMergeSet.HasDeletedTSID(id)`，跳过该 series。
   - 调用 `MsBuilder.WriteRecord` 写入 record；新 extent 文件名使用 `extent + 1`。
5. 若 builder 有数据，调用 `NewTSSPFile(true)` 并追加到 builder files。
6. 若 builder 无数据，删除空临时文件。
7. 返回新文件列表。

### 3.14 MmsTables.ReplaceFiles

```go
func (m *MmsTables) ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool) error
```

流程：

1. 若 `len(newFiles) == 0 || len(oldFiles) == 0`，直接返回 nil。
2. defer recover，将 panic 转为 `errno.RecoverPanic`。
3. 写 compact log：`writeCompactedFileInfo`。
4. 调用 `RenameTmpFiles(newFiles)`。
5. 获取 ordered 文件集合。
6. 加 `fs.lock.Lock()`。
7. 对每个旧文件：
   - 若 store 关闭或 stop compact/merge，返回 `ErrCompStopped`。
   - 从内存文件集合删除。
   - 调用 `m.deleteFiles(f)` 删除物理文件；如果文件仍被使用，会 rename 为 tmp 并加入 GC。
8. append new files。
9. sort 文件集合。
10. 删除 compact log。

恢复：

- compact log 由 `compaction_file_info.go` 的恢复流程使用。

### 3.15 NonStreamingCompaction

```go
func NonStreamingCompaction(fi FilesInfo) bool
```

返回：

- true: 使用 `ChunkIterators` + `MmsTables.compact`。
- false: 使用 `StreamIterators.compact`。

判断：

1. `CorrectTimeDisorder == true` -> true。
2. `GetMergeFlag4TsStore() == util.NonStreamingCompact` -> true。
3. `GetMergeFlag4TsStore() == util.StreamingCompact` -> false。
4. 自动判断：
   - `fi.avgChunkRows * fi.maxColumns * 8 * len(fi.compIts) >= 128MB` -> false。
   - `fi.maxChunkRows > GetMaxRowsPerSegment4TsStore() * 500` -> false。
   - 否则 true。

## 4. 状态机

### 4.1 文件路径占用状态

```text
          genCompactGroup
                |
                v
        busy(group.files)?
          |           |
        yes          no
          |           |
        skip      scheduler task
                      |
                      v
              BeforeExecute/acquire
                 |          |
              failed       ok
                 |          |
                skip     execute
                            |
                            v
                    Finish/CompactDone
```

### 4.2 Full compact 计数

```text
FullCompact
    |
    v
n = maxFullCompactor - fullCompactingCount
    |
    v
build plans up to n measurement groups
    |
    v
CompactTask.Execute(full=true, group.Len > 1)
    |
    +--> IncrFull(+1)
    |
    +--> compactToLevel
    |
    +--> IncrFull(-1)
```

## 5. 错误处理

| 错误/场景 | 来源 | 当前处理 |
| --- | --- | --- |
| `ErrCompStopped` | store closed 或 stop compact/merge | 返回到 task，task 记录 compact error。 |
| iterator 创建失败 | `NewFileIterators` | 记录错误，增加 compact error 统计，任务返回。 |
| compact 执行失败 | `compactToLevel` | 记录错误，增加 compact error 统计。 |
| replace 失败 | `ReplaceFiles` | 返回错误，compactToLevel 记录并返回。 |
| 文件仍被查询使用 | `deleteFiles` | rename 为 tmp，加入 GC，避免直接删除正在使用的文件。 |
| no new files | `ReplaceFiles` | 直接返回 nil，不替换旧文件。 |

## 6. 统计信息

`tsImmTableImpl.compactToLevel` 创建并推送 `CompactStatItem`：

```go
type CompactStatItem struct {
    Level              uint16
    Full               bool
    OriginalFileCount  int64
    OriginalFileSize   int64
    CompactedFileCount int64
    CompactedFileSize  int64
}
```

统计字段在 compact 成功并且 old file size 非 0 时填充。`compactStat.AddActive` 记录活跃任务数，defer 中调用 `compactStat.PushCompaction`。

## 7. 实现约束

- 本文档的 TSSTORE compact 只处理 `m.Order`。
- `LevelCompact` 的目标 level 是 `level + 1`。
- 普通 `FullCompact` 的目标 level 由输入文件最大 level 推进，不保证直接到 level 6。
- `mmsPlan` 按唯一 sequence 形成组，遇到重复 sequence run 会跳过并 reset。
- `ReplaceFiles` 是内存文件集合与物理文件替换的唯一入口。
- 任何新 compact 模式都必须保持 compact log 可恢复语义。

## 8. 测试映射

| 测试文件 | 覆盖点 |
| --- | --- |
| `compact_test.go` | level compact、full compact、并发参数、替换和恢复相关场景。 |
| `stream_compact_test.go` | 流式 compact、`NonStreamingCompaction` 判断、segment 级处理。 |
| `tssp_reader_test.go` | compact 后文件读取、plan 分组等行为。 |
| `task_test.go` | compact task、full compact 单文件/多文件路径。 |

## 9. 参考代码

- `engine/shard.go:Compact`
- `engine/immutable/compact.go`
- `engine/immutable/task.go`
- `engine/immutable/ts_mms_tables.go`
- `engine/immutable/mms_tables.go`
- `engine/immutable/stream_compact.go`
- `engine/immutable/compaction_file_info.go`

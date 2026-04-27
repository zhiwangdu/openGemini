# openGemini out-of-order merge spec

本文档描述 `engine/immutable` 中乱序数据合并逻辑，分析起点为 `merge_performer.go`。文档覆盖两条路径：

- 乱序合并到有序：out-of-order TSSP files merge into ordered TSSP files。
- 乱序自合并：out-of-order TSSP files merge into out-of-order TSSP files。

## 术语

| 术语 | 含义 |
| --- | --- |
| ordered file | 正常有序 TSSP 文件，保存在 measurement 目录下。 |
| unordered file | 乱序 TSSP 文件，保存在 measurement 的 `out-of-order` 子目录下。 |
| series | TSSP chunk 对应的 series id。合并以 series id 为主序。 |
| column | series 内字段列。time 列总是 chunk 的最后一列。 |
| segment | column 的物理分段。`columnWriter` 会按 `GetMaxRowsPerSegment4TsStore()` 重新切段。 |
| merge level | 文件名中的 merge level。乱序自合并会提升 level。 |

## 总体入口

`MmsTables.MergeOutOfOrder(shId, full, force)` 是后台合并入口：

1. `getMstToMerge` 选择需要合并的 measurement。
2. `mergeOutOfOrder` 为每个 measurement 构造 `MergeContext`。
3. `execMergeContext` 创建 `mergeTool`。
4. `mergeTool` 根据 `ctx.MergeSelf()` 选择：
   - `merge(ctx)`：乱序合并到有序。
   - `mergeSelf(ctx)`：乱序自合并。

`MergeContext` 的构造规则：

- `force == true`：直接构造普通 merge context，用于把乱序合并到 ordered。
- `full == true`：构造全量自合并 context，优先合并低于 parquet level 的乱序文件。
- `Merge.MergeSelfOnly` 或 `UnorderedOnly`：仅构造自合并 context。
- 普通后台模式先按 level 构造自合并 context；没有自合并任务且到达数量或时间阈值时，再构造普通乱序到有序 context。

## 乱序合并到有序

### 文件选择

`mergeTool.mergePrepare` 先调用 `MmsTables.matchOrderFiles(ctx)` 匹配 ordered 文件。

匹配策略：

1. 遍历当前 measurement 的 ordered 文件。
2. 若 ordered 文件时间范围与乱序 context 时间范围重叠，则加入 `ctx.order`。
3. 一旦已加入过 ordered 文件，后续 ordered 文件也加入。
4. 若没有任何重叠文件，则加入最后一个 ordered 文件。

第 3 点很关键：当乱序数据可能跨越多个 ordered 文件边界时，合并会从第一个命中文件开始连续重写后续 ordered 文件，确保剩余乱序数据有机会落入正确的 ordered 文件。

### 执行框架

`mergeTool.execute(mst, order, unordered)` 是乱序合并到有序的核心执行函数：

1. 创建一个共享的 `UnorderedReader` 并添加所有 unordered 文件。
2. 对每个 ordered 文件创建一个输出 `StreamWriteFile`，文件名基于原 ordered 文件。
3. 对每个 ordered 文件创建一个 `mergePerformer`，绑定：
   - 共享 `UnorderedReader`
   - 输出 `StreamWriteFile`
   - 当前 ordered 文件的 `ColumnIterator`
4. 将所有 performer 放入 `MergePerformers` 最小堆。
5. 循环调用 `MergePerformers.Next()`，直到所有 performer 完成。

`MergePerformers` 的堆排序规则：

1. `sid` 小的 performer 先处理。
2. `sid` 相同时，当前 chunk `minTime` 小的 performer 先处理。

这个顺序保证多 ordered 文件并行参与时，整体按 series id 和时间推进，共享的 `UnorderedReader` 可以单调消费乱序数据。

### series 级调度

`MergePerformers.Next()` 每次处理一组相同 sid：

1. 从堆中弹出最小 performer。
2. 如果这是本轮第一个 sid：
   - 调用 `item.writeRemain(sid)`，先写出所有 unordered 中 sid 小于当前 ordered sid 的剩余数据。
   - 调用 `item.ur.ChangeSeries(sid)`，把所有 unordered reader 推进到当前 sid。
3. 调用 `itr.IterCurrentChunk(item)` 处理当前 ordered chunk。
4. 调用 `item.finishSeries()` 写完当前 series 元数据。
5. 若 ordered iterator 还有 chunk，则更新 performer 的 sid 后重新入堆。
6. 若 ordered 文件已完成，则调用 `item.Finish()` 生成新的 TSSP file。

`MergePerformers.Pop()` 会为弹出的 performer 设置两个标志：

- `lastSeries`：当前堆中没有其他 performer 处理相同 sid 时为 true。
- `lastFile`：当前堆为空时为 true。

这两个标志用于决定是否读取到 `math.MaxInt64` 或写出所有剩余 unordered 数据。

### ordered chunk 读取与短路

`ColumnIterator.IterCurrentChunk(p)` 处理 ordered chunk：

1. 如果 `p.HasSeries(sid)` 为 false，表示 unordered 当前没有该 sid：
   - 调用 `p.SeriesChanged(sid, nil)`。
   - 调用 `p.WriteOriginal(fi)`，直接复制 ordered chunk 的原始数据和 meta。
   - 该路径避免无谓解码和重编码。
2. 如果 unordered 有该 sid：
   - 先读取 ordered time 列。
   - 调用 `p.SeriesChanged(sid, orderedTimes)`。
   - 逐列、逐 segment 调用 `ColumnChanged` 和 `Handle`。

### mergePerformer 的状态机

`mergePerformer` 是 ordered chunk 与 unordered 数据的合并执行器。

#### SeriesChanged

输入当前 ordered sid 和 ordered time 列：

1. 统计 ordered series 数。
2. ordered time 为空时，只更新 sid 并返回。
3. 计算 unordered 可读取上界：
   - 普通情况：`maxOrderTime = orderedTimes[len-1]`。
   - 若 `lastSeries == true`：`maxOrderTime = math.MaxInt64`。
4. `UnorderedReader.InitTimes(sid, maxOrderTime)` 合并所有 unordered 文件中该 sid、时间不超过上界的 time。
5. 如果没有 unordered time：
   - `noUnorderedSeries = true`。
   - `mergedTimes = orderedTimes`。
6. 如果存在 unordered time：
   - 读取 unordered schema。
   - `mergedTimes = MergeTimes(orderedTimes, unorderedTimes)`。
   - 统计 intersect series 数。
7. 将 `mergedTimes` 写入 `mergedTimeCol`，切换输出 sid。

`MergeTimes` 会合并两个有序时间数组，并对同一时间只保留一个时间点。

#### ColumnChanged

输入当前 ordered 字段列：

1. 默认认为当前列在 unordered 中不存在：`noUnorderedColumn = true`。
2. 如果当前 series 没有 unordered 数据，或 unordered schema 为空：
   - 切换 unordered reader 当前列。
   - 在输出文件追加当前 ordered 列。
3. 如果 unordered schema 中存在字典序小于当前 ordered 列名的字段：
   - 这些字段只存在于 unordered。
   - 调用 `writeUnorderedCol` 先补写这些列。
4. 如果 unordered schema 中存在同名字段：
   - 设置 `noUnorderedColumn = false`。
5. 消费已经处理过的 unordered schema。
6. 切换 unordered reader 当前列，并在输出文件追加当前 ordered 列。

该逻辑依赖字段 schema 按列名字典序排序，time 列最后写。

#### Handle

输入当前 ordered 列的一个 segment：

1. 如果当前 series 没有 unordered 数据，直接写 ordered segment。
2. 计算当前 segment 的 unordered 读取上界：
   - 普通情况：当前 segment 最后一个 ordered time。
   - 当前 ordered 文件中该 sid 的最后一个 segment 且 `lastSeries == true`：`math.MaxInt64`。
3. 调用 `readUnordered(maxOrderTime)`：
   - 当前列存在于 unordered：读取真实 unordered 列值。
   - 当前列不存在于 unordered：读取 unordered time，并创建等长 nil 列。
4. 调用 `merge(orderCol, unorderedCol, orderTimes, unorderedTimes, ref, lastSeg)`。

#### merge

`mergePerformer.merge` 的规则：

1. 若本时间范围没有 unordered time，直接写 ordered col。
2. 否则把 unordered col 和 unordered times 加入 `record.MergeHelper`。
3. 调用 `MergeHelper.Merge(orderCol, orderTimes, ref.Type)` 得到 merged col 和 merged times。
4. 写入输出列。

`record.MergeHelper` 的冲突规则：

- `order.time < unordered.time`：写 ordered。
- `unordered.time < order.time`：写 unordered。
- `order.time == unordered.time`：
  - unordered 值非 nil 时，unordered 覆盖 ordered。
  - unordered 值为 nil 时，保留 ordered。

#### finishSeries

当前 ordered series 的所有字段列处理完后：

1. `writeRemainCol()` 写出 unordered schema 中还未遇到的字段列。
2. `writeMergedTime()` 追加并写入 time 列。
3. `sw.WriteCurrentMeta()` 写当前 sid 的 chunk meta。

#### writeUnorderedCol

用于写只存在于 unordered 的字段：

1. 切换 unordered reader 当前列。
2. 输出文件追加该字段列。
3. 以 `mergedTimes` 创建同长度 nil ordered 列。
4. 读取 unordered 列。
5. 调用 `merge(nilOrderedCol, unorderedCol, mergedTimes, unorderedTimes, ref, true)`。

结果是：该字段在非 unordered 时间点上为 nil，在 unordered 时间点上使用 unordered 值。

#### writeRemain

`writeRemain(maxSid)` 写出 unordered 中 sid 小于 `maxSid` 的剩余数据。

典型调用点：

- 处理某个 ordered sid 前：写出所有更小 sid 的 unordered 数据。
- 最后一个 ordered 文件 `Finish()` 时：用 `math.MaxUint64` 写出所有剩余 unordered 数据。

写出流程：

1. `UnorderedReader.ReadRemain(maxSid, callback)` 找到 unordered readers 当前持有的最小 sid。
2. 读取该 sid 的 schema 和完整 time。
3. 对 schema 中每个字段按 segment 大小读取列数据。
4. callback 写数据列。
5. 最后 callback 写 time 列。

该路径覆盖 unordered 中完全没有对应 ordered sid 的数据。

### UnorderedReader

`UnorderedReader` 是多个 unordered 文件的共享读取器。每个文件对应一个 `UnorderedColumnReader`。

#### ChangeSeries

`UnorderedColumnReader.ChangeSeries(sid)` 的行为：

- 如果当前 chunk 已经全部读取完成，将本 reader 的 sid 置 0。
- 如果当前 reader 的 sid 等于目标 sid，保持不动。
- 如果当前 reader 的 sid 大于 0 且不是目标 sid，保持不动，表示该 reader 当前停在未来 sid。
- 如果当前 reader 没有 sid，则读取下一个 chunk meta，初始化列索引和 time 列。

因此 unordered reader 不会倒退，只能随着 merge 调度单调前进。

#### InitTimes

`UnorderedReader.InitTimes(sid, maxTime)` 对所有 child reader：

1. 调用 `reader.ReadTime(sid, maxTime)`。
2. 将各 reader 返回的 time 通过 `MergeTimes` 合并到 `r.times`。

`ReadTime` 会推进 time 列的 line offset，表示这些 time 已进入当前 series 的待合并时间域。

#### Read

`UnorderedReader.Read(sid, maxTime)` 读取当前字段列：

1. 先用 `ReadTimes(maxTime)` 从全局 merged unordered time 中取出本次时间窗口。
2. 创建同长度 nil 列作为基础列。
3. 逐个 child reader 读取该字段在窗口内的数据。
4. 用 `UnorderedReaderContext.merge` 把多个 unordered 文件的列值合并到 nil 基础列。

多个 unordered 文件之间如果存在同一时间点，后续加入 `MergeHelper` 的数据会按相同冲突规则覆盖先前结果。因此最终值依赖 `UnorderedReader.readers` 的文件顺序；该顺序来自 `ctx.unordered.Sort()` 后的路径顺序。

### 输出与替换

乱序合并到有序完成后：

1. `mergeTool.execute` 返回 `mergedFiles`。
2. `mergeTool.merge` 调用 `replaceMergedFiles`。
3. `replaceMergedFiles` 只替换 seq 和 extend 与新文件匹配的旧 ordered 文件。
4. `ReplaceFiles(..., isOrder=true)`：
   - 写 compact log。
   - 将新文件从临时名 rename 为正式名。
   - 从 ordered 文件集合删除旧文件并删除物理文件。
   - 加入新文件并排序。
   - 删除 compact log。
5. `deleteUnorderedFiles` 删除本次参与合并的 unordered 文件。

## 乱序自合并

`mergeTool.mergeSelf(ctx)` 只处理 unordered 文件集合。若 `ctx.UnorderedLen() <= 1`，直接返回。

自合并有两种模式：

- fast mode：`ctx.MergeSelfFast() == true`。
- stream mode：`ctx.MergeSelfFast() == false`。

判断规则：

```go
ctx.ToLevel() == config.TSSPToParquetLevel() ||
int(ctx.ToLevel()) <= config.GetStoreConfig().Merge.StreamMergeModeLevel
```

也就是说，目标 level 到达 parquet 转换 level，或目标 level 不大于 `StreamMergeModeLevel` 时使用 fast mode；其他情况使用 stream mode。

### fast mode

`mergeSelfFastMode` 的流程：

1. 根据 context 路径取出 unordered 文件。
2. 创建 `MergeSelf`。
3. 初始化 merge-self event，用于 parquet 相关流程或索引协作。
4. 监听 shard close / stop 信号。
5. 调用 `MergeSelf.Merge(mst, ctx.ToLevel(), files)`。
6. 触发 replace-file event。
7. `ReplaceFiles(..., isOrder=false)` 用一个新的 unordered 文件替换旧 unordered 文件集合。

`MergeSelf.Merge` 的数据路径：

1. 用第一个输入文件名创建输出 `MsBuilder`，并把文件名 merge level 改为 `toLevel`。
2. 为每个 unordered 文件创建 `ChunkIterator`。
3. `ChunkIterators` 按 `(series id, record min time)` 建最小堆。
4. 循环调用 `itrs.Next()`：
   - 弹出最小 series 的 record。
   - 聚合同一 series id 的所有 record。
   - 若有更多 chunk，iterator 重新入堆。
5. 对聚合后的 record 调用 `record.ColumnSortHelper.Sort(rec)`。
6. 调用 `builder.WriteRecord(sid, rec, nil)` 写入新 unordered 文件。
7. 完成后 `builder.NewTSSPFile(true)` 生成文件并触发 new-file event。

fast mode 是 record 级多路归并。它会把相同 series 的 record 合成一个 record，再按列排序和时间顺序重排，然后写入一个更高 merge level 的 unordered 文件。

### stream mode

`mergeSelfStreamMode` 复用乱序到有序的执行框架，但输入输出都在 unordered 文件集合内。

流程：

1. 从 context 取出 unordered 文件。
2. 将第一个 unordered 文件放入 `order` 集合。
3. 将其余 unordered 文件放入 `unordered` 集合。
4. 调用 `mergeTool.execute(ctx.mst, order, unordered)`。
5. `ReplaceFiles(..., isOrder=false)` 用 merged 文件替换第一个 unordered 文件。
6. 删除其余参与合并的 unordered 文件。

这里的 “order” 只是 `mergeTool.execute` 的参数角色，实际文件仍然属于 out-of-order 集合，替换时 `isOrder=false`。因此 stream mode 的语义是：以第一个 unordered 文件为骨架，把其余 unordered 文件合并进去，产出新的 unordered 文件。

### 自合并 context

自合并 context 由 `BuildMergeContext` 生成：

- `buildLevelMergeContext`：按相同 merge level 聚合 unordered 文件，到达 level 配置的文件数阈值后触发。
- `buildFullMergeContext`：全量模式下按大小和目标 level 聚合。
- `buildUnorderedOnlyMergeContext`：`MergeSelfOnly` 或 `UnorderedOnly` 时遍历所有已有 level。

文件数阈值：

- 默认 `DefaultLevelMergeFileNum = 4`。
- `LevelMergeFileNum = []int{8, 8}` 覆盖 level 0 和 level 1 的阈值。

## 数据正确性约束

### 时间有序与去重

合并后的 time 列必须有序。相同时间点只保留一个行位置。

约束来源：

- unordered 文件内部 time 已按 TSSP chunk 有序。
- `MergeTimes` 合并 ordered 和 unordered time。
- `MergeHelper` 对列值按 time 顺序写出。
- fast self merge 中 `ColumnSortHelper.Sort` 对 record 排序。

### 同时间覆盖规则

当 ordered 和 unordered 存在同一时间点：

- unordered 非 nil 值覆盖 ordered 值。
- unordered nil 值不覆盖 ordered，保留 ordered 值。

当多个 unordered 文件存在同一时间点，后加入 `MergeHelper` 的列值按同样规则覆盖先前结果。

### schema 并集

合并输出 schema 是 ordered schema 与参与窗口内 unordered schema 的并集：

- ordered 独有字段：unordered 时间点补 nil。
- unordered 独有字段：ordered 时间点补 nil。
- 两边共有字段：按 time 合并并应用覆盖规则。
- time 列最后写入。

### series 完整性

合并输出必须覆盖三类 series：

- ordered 和 unordered 都存在的 sid：逐列合并。
- 只存在于 ordered 的 sid：可直接复制原始 chunk。
- 只存在于 unordered 的 sid：由 `writeRemain` 写出。

### 文件集合原子性

文件替换通过 compact log 和临时文件 rename 完成：

- 新文件先以临时文件形式写入。
- 替换前写 compact log。
- rename 新文件后更新内存文件集合。
- 删除旧文件和 compact log。

若过程中失败，恢复流程可依据 compact log 清理或完成替换。

## 已知边界与实现注意事项

- `UnorderedReader` 是共享的有状态 reader，必须按 `MergePerformers` 的 sid 顺序单调消费，不支持并发调用。
- `writeRemain(maxSid)` 会消费 sid 小于 `maxSid` 的 unordered 数据；调用时机错误会导致乱序数据提前或遗漏写出。
- `lastSeries` 决定当前 sid 是否可以读取到 `math.MaxInt64`。如果同 sid 仍存在后续 ordered chunk，不能越界消费未来数据。
- `lastFile` 只在最后一个 performer 完成时写出所有剩余 unordered 数据。
- `WriteOriginal` 会直接复制 chunk 数据并修正 offset，不做列解码；只有当前 unordered reader 不包含该 sid 时才能使用。
- `ColumnChanged` 依赖 schema 字典序。新增列排序规则时必须同步检查该逻辑。
- stream self merge 使用第一个 unordered 文件作为骨架；输入文件顺序由 merge context 排序决定。
- fast self merge 会将多个 unordered 文件压成一个文件；stream self merge 每次只替换第一个文件并删除其余文件。

## 关键测试映射

| 测试 | 覆盖点 |
| --- | --- |
| `TestMergeTool_Merge_mod1` | 多个 unordered 文件合并进一个 ordered 文件。 |
| `TestMergeTool_Merge_mod2` | ordered 文件之间的剩余 unordered 数据处理。 |
| `TestMergeTool_Merge_mod3` | ordered / unordered schema 不一致。 |
| `TestMergeTool_Merge_mod4` | 多 segment 合并和重新切段。 |
| `TestMergeTool_Merge_mod5` | 单文件多 series。 |
| `TestMergeTool_Merge_mod6` | 大量 series 和不同 schema。 |
| `TestMergeTool_Merge_mod7` | unordered 时间或 sid 超出 ordered 范围时的剩余写出。 |
| `TestMergeTool_MergeUnorderedSelf` | unordered 自合并 level 推进和文件数收敛。 |
| `TestMergeSelf` | fast self merge 后继续合并到 ordered 的结果正确性。 |
| `TestMergeSelf_Stop` | self merge 停止信号与异常文件场景。 |

## 参考代码

- `engine/immutable/merge_out_of_order.go`：入口、context 执行、文件替换辅助。
- `engine/immutable/merge_util.go`：`MergeContext` 构造和 `MergeTimes`。
- `engine/immutable/merge_tool.go`：merge / merge-self 编排。
- `engine/immutable/merge_performer.go`：乱序合并到有序的逐列执行器。
- `engine/immutable/unordered_reader.go`：多个 unordered 文件的按需读取和合并。
- `engine/immutable/column_iterator.go`：ordered 文件按 chunk、column、segment 迭代。
- `engine/immutable/merge_self.go`：fast self merge。
- `engine/immutable/chunk_iterators.go`：record 级多路归并。
- `lib/record/meger.go`：列值按 time 合并及同时间覆盖规则。

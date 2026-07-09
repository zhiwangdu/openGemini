# Step 1 堆式乱序合并：核心场景压测报告

> 本报告记录 `tsmMergeCursor.FirstTimeInit` 乱序合并从链式 `MergeRecord` 改为堆式 K 路归并（Step 1）
> 在核心场景下的 CPU / GC / 耗时对比，以及一项配套的预分配优化。所有数据来自 Go 微基准
> （`engine/unordered_heap_merge_bench_test.go`，`-benchmem`），精确隔离 unordered 内部合并的算法差异，
> 不含 e2e 的 HTTP/序列化/I/O 开销。

---

## 1. 测试场景（核心场景）

方案 §1.3 的核心收益场景：

- **大 string 列**：1KB / 3KB / 5KB / 7KB / 9KB
- **多乱序文件**：K（文件数）= 10（矩阵）/ 10–300（K 扫描）
- **乱序间几乎全重叠**：时间戳跨文件交错（file i row j → `j·K + i`），所有文件覆盖同一时间范围，
  但跨文件 timestamp 不重复（高范围重叠 + 不大量重复）
- **86400 点有序/乱序各占一半**：本基准的 M 为**乱序点数**（heap 的输入；另一半有序点由下游
  `mergeData` 合并，heap/chain 不变，不计入差异）
- 升序、非聚合

堆合并：每段一个 source，K 路堆按时间 pop，同 timestamp 跨段折叠（newest 优先级胜出），输出 `outRec`。
链式合并（baseline）：逐段 `MergeRecord(rec, outRec)`，每次重扫累计 `outRec`。

---

## 2. 预分配优化

### 2.1 问题

heap 逐行 `AppendColVal` 构造 `outRec` 的 string 列，其 `[]byte` 从 0 增长到 `M·S`。Go 对**大 slice**
的 append 增长系数收敛到 ~1.25×（非 2×），故累计分配量 ≈ **~6× 最终大小**。实测优化前 heap B/op ≈
**5.7·M·S**（远高于理论 outRec 的 1·M·S）。

### 2.2 优化

合并前按输入总量一次性预留 `outRec` 各列容量（`Val` 按 `Σ source.Val 长度`、`Offset`/`Bitmap` 按总行数 M），
之后逐行 append 不再触发扩容：

```go
totalRows := Σ source.RowNums()
totalValBytes[c] := Σ len(source.ColVals[c].Val)
out.ColVals[c].Val    = make([]byte, 0, totalValBytes[c])
out.ColVals[c].Offset = make([]uint32, 0, totalRows)
out.ColVals[c].Bitmap = make([]byte, 0, totalRows/8+1)
```

输出有去重时（同 timestamp 折叠）预留容量会富余，但仍只 1 次分配、无扩容开销。

### 2.3 验证（heap B/op：优化前 → 优化后）

| 组合 | 优化前 | 优化后 | 降幅 | 理论 1·M·S |
|---|---|---|---|---|
| S1KB, M86400 | 497 MB | **90 MB** | 5.5× | 86 MB ✓ |
| S9KB, M172800 | 9.65 GB | **1.60 GB** | 6.0× | 1.59 GB ✓ |

heap B/op 降到 **~1·M·S**（outRec 最终大小）；`allocs/op` 从 ~170 降到 **27**。heap CPU 也因免去扩容拷贝
大幅改善（S9KB M172800：2592ms → 335ms，7.7×）。差分测试全通过，正确性不变。

---

## 3. S × M 矩阵（K=10，预分配后）

`-benchtime=1x`，K=10，R=M/10，交错时间戳（高重叠不重复）。

### 3.1 CPU 耗时（ms，heap / chain，加速比）

| S \ M | 86400 | 129600 | 172800 |
|---|---|---|---|
| 1KB | 71 / 500 (**7.0×**) | 124 / 888 (**7.2×**) | 140 / 1106 (**7.9×**) |
| 3KB | 123 / 1497 (**12×**) | 132 / 2285 (**17×**) | 181 / 2977 (**16×**) |
| 5KB | 114 / 2086 (**18×**) | 161 / 3420 (**21×**) | 213 / 4476 (**21×**) |
| 7KB | 135 / 2829 (**21×**) | 200 / 4849 (**24×**) | 274 / 6571 (**24×**) |
| 9KB | 202 / 4248 (**21×**) | 307 / 5751 (**19×**) | 335 / 9242 (**28×**) |

### 3.2 分配量 B/op（GB，heap / chain，倍数 = GC 压力代理）

| S \ M | 86400 | 129600 | 172800 |
|---|---|---|---|
| 1KB | 0.090 / 2.73 (**30×**) | 0.135 / 4.23 (**31×**) | 0.180 / 5.35 (**30×**) |
| 3KB | 0.267 / 8.14 (**30×**) | 0.400 / 12.43 (**31×**) | 0.534 / 15.91 (**30×**) |
| 5KB | 0.444 / 13.38 (**30×**) | 0.666 / 20.66 (**31×**) | 0.888 / 26.15 (**29×**) |
| 7KB | 0.621 / 18.54 (**30×**) | 0.931 / 27.92 (**30×**) | 1.241 / 38.68 (**31×**) |
| 9KB | 0.798 / 24.13 (**30×**) | 1.197 / 34.93 (**29×**) | 1.595 / 47.93 (**30×**) |

### 3.3 观察

- 分配倍数 ~**30× 恒定**（= 3·K，K=10）——heap 降到 1·M·S，chain 仍 30·M·S。与 S、M 无关，仅取决于 K。
- CPU 加速比 **7–28×**，随 S 增大而增大（大 string 放大 chain 重扫代价）；最大组合（9KB/M172800）heap 自身
  分配仅 1.6GB，不再有优化前的 GC 超线性问题。

---

## 4. K 维度扫描（S=5KB, M=4320，K=10/50/100/300）

M 较矩阵缩小到 4320，使 chain 的 `O(K·M·S)` 分配在 K=300 仍可行（live 内存 ~3·M·S = 65MB 恒定）。

| K | heap ms | chain ms | **CPU 加速** | heap MB | chain GB | **分配倍数** | allocs heap / chain |
|---|---|---|---|---|---|---|---|
| 10 | 8.8 | 75.9 | **8.6×** | 22.2 | 0.67 | **30×** | 27 / 723 |
| 50 | 6.9 | 387 | **56×** | 22.1 | 3.06 | **139×** | 69 / 3511 |
| 100 | 8.7 | 777 | **89×** | 22.1 | 5.96 | **270×** | 120 / 6550 |
| 300 | 10.9 | 2560 | **234×** | 21.6 | 16.2 | **750×** | 322 / 16391 |

### 4.1 观察

- **heap B/op 与 K 无关**（~22MB 恒定 = M·S）——heap 是 `O(M·S)`，与文件数无关。
- **chain B/op 随 K 线性增长**（0.67GB → 16.2GB，K 10→300 增 24× ≈ 线性）——chain 是 `O(K·M·S)`。
- **分配倍数 ∝ K**：30× → 139× → 270× → 750×（K 10→300，25× ≈ 线性于 K）。
- **CPU**：heap ~恒定（7–11ms，`O(M·logK)`，logK 几乎不变）；chain 随 K 线性（76 → 2560ms，34×）；
  加速比 8.6× → 234×，随 K 发散。
- `allocs/op`：heap 27–322（K 个 source 结构，bytes 恒定）；chain 723–16391（K 次 MergeRecord 各分配 outRec）。

---

## 5. 结论

1. **预分配优化有效**：heap B/op 从 ~5.7·M·S 降到 ~1·M·S（5.7×），allocs/op 从 ~170 降到 27，CPU 亦改善。
2. **核心场景 heap 全面碾压 chain**：
   - S×M 矩阵：CPU **7–28×**，分配 **~30×**（恒定）。
   - K 扫描：随文件数 K 发散——K=300 时 CPU **234×**、分配 **750×**。
3. **复现理论发散**：chain `O(N²R)=O(K·M·S)` 随 K 线性恶化；heap `O(M·logK)` 与 K 几乎无关。**乱序文件越多，
   heap 优势越大**——这正是优化的核心动机。
4. **GC 压力**：heap 分配量 ~1·M·S（与 K 无关），chain ~30·M·S·(K/10)（随 K 线性）；heap 的 GC 对象数也低 1–2 个数量级。
5. **内存安全**：所有组合 live 峰值 ~3·M·S（K 扫描恒定 65MB；矩阵最大 9KB/M172800 ≈ 4.8GB），chain 的 B/op 是
   分配量（GC 边分配边回收），live 不随之增长。

---

## 6. 复现

```bash
make failpoint-enable
# S×M 矩阵
go test ./engine -run '^$' -bench 'BenchmarkUnorderedMergeCore$' -benchtime=1x -benchmem
# K 扫描
go test ./engine -run '^$' -bench 'BenchmarkUnorderedMergeKSweep$' -benchtime=1x -benchmem
make failpoint-disable
```

环境：12 核 CPU，31GB RAM（22GB available）。矩阵最大组合 live 峰值 ~4.8GB；K 扫描 live 恒定 65MB。

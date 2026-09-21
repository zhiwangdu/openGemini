# 10 KiB 字符串 memtable 内存调查

[调查报告](REPORT.md) · [逐轮汇总](results.csv) · [三次重复统计](evidence/rollup.json) · [测试记录](validation/results.json)

这是调查与实验分支，不是生产修复分支。诊断代码仅在 `memdiag` build tag 下启用；优化 PoC 默认关闭。大列翻倍扩容在部分容量边界会增加存活堆，分块方案仅完成布局微基准，详见报告。

## 已归档的内容

- 仓库中的 Go 诊断代码、试验开关和回归测试。
- `scripts/`：单机压测、参数矩阵、回归测试、容量微基准、分析和审计。
- `patches/`：相对于原始基线的诊断补丁、实验补丁，便于分别重建两种二进制。
- `results.csv`、`evidence/`、`validation/`：56 轮成功实验的汇总、pprof 文本、环境、测试日志；130 个 retained 检查点审计通过。
- `evidence/checkpoints/`：基线、强制归还空闲页、查询、32 MiB snapshot 对照的关键原始 JSON，以及首次查询失败记录；不是全部运行目录。
- `figures/`：报告图表。

原始完整实验目录为 `/home/duzhiwang/workspace/data/openGeminiSingle`。二进制、heap profile、WAL/TSSP、服务日志和虚拟环境没有提交。JSON 中的绝对路径和 `manifest.json` 的哈希记录属于原始实验，报告中的相对链接已针对仓库调整；不能把原始文档哈希用于校验当前仓库版报告。

## 现有机器上继续实验

原实验目录的代码 worktree 就是当前分支，已有二进制与全部原始证据。无需重复应用补丁：

```bash
cd /home/duzhiwang/workspace/data/openGeminiSingle
python3 scripts/run.py --name repro_baseline_02
python3 scripts/run.py --name repro_doubling_02 --binary bin/ts-server-validated --variant doubling
```

每次使用不存在的名称，脚本不会覆盖历史实验。固定回环端口为 18086/18088/18090/18091/18092/18400/18401，同一时刻只运行一轮。

## 从干净基线重建独立实验目录

需要 Go 1.26.4（原始实验工具链）、Python 3.11+、Linux。下面两个路径按机器实际位置修改，实验目录必须尚不存在。不要把 WAL、data 或二进制放入生产目录。

```bash
MEMLAB_REPO=/absolute/path/to/openGemini
MEMLAB_DIR=/absolute/path/to/new-openGeminiSingle

# mkdir 不使用 -p：目标已存在时应停止，避免混合历史数据。
mkdir "$MEMLAB_DIR"
cp -R "$MEMLAB_REPO/benchmarks/memtable-string/." "$MEMLAB_DIR/"
mkdir "$MEMLAB_DIR/bin"
git -C "$MEMLAB_REPO" worktree add --detach "$MEMLAB_DIR/source" \
  7fded2ceaf17f70a0cfbd0f8753837c26b5858c5

cd "$MEMLAB_DIR/source"
git apply --check ../patches/instrumentation.patch
git apply ../patches/instrumentation.patch
go build -mod=readonly -buildvcs=false -tags memdiag -o ../bin/ts-server-baseline ./app/ts-server

git apply --check ../patches/experiments.patch
git apply ../patches/experiments.patch
go build -mod=readonly -buildvcs=false -tags memdiag -o ../bin/ts-server-experiments ./app/ts-server
cp ../bin/ts-server-experiments ../bin/ts-server-validated
```

这些补丁只能用于上述干净基线。当前调查分支已包含对应修改，不能再次应用；直接在当前分支构建会得到实验版，开关全关的 control 与仅加诊断的 baseline 已做对照。

`run.py` 根据脚本所在目录确定实验根目录，其 `source/` 是上述独立 worktree。不要直接在当前仓库的 `benchmarks/memtable-string/` 中运行完整实验。

## 运行与验证

```bash
cd "$MEMLAB_DIR"
python3 scripts/run.py --name fresh_baseline
python3 scripts/run.py --name fresh_doubling --binary bin/ts-server-validated --variant doubling
python3 scripts/run.py --name fresh_boundaries --binary bin/ts-server-validated --variant doubling --sizes 76,150,300
python3 scripts/run.py --name fresh_memlimit --binary bin/ts-server-validated --gomemlimit 512MiB
python3 scripts/run.py --name fresh_recovery --binary bin/ts-server-validated --variant doubling --sizes 8 --pattern escaped --verify-full --recovery

# 完整矩阵；已成功完成的名称会跳过，失败目录保留。
python3 scripts/matrix.py
python3 scripts/optimize.py
python3 scripts/edges.py
python3 scripts/operational.py
python3 scripts/validate.py
python3 scripts/analyze.py
python3 scripts/audit.py
```

默认单 field 10 KiB、单 series、约 256 MiB 总 payload，WAL 开启。脚本设置 shard mutable=1 GiB、node mutable=2 GiB、write-cold=1h、force-snapshot=1h、full-compaction cold=24h，并验证 retained 阶段无 flush、无 TSSP。TSSTORE 不能只靠 force-snapshot 配置阻止冷写 flush。每个检查点的原始状态在 GC 前采集，再做两次 GC；最终才主动 flush 并读回。

`--sizes` 表示累计目标 MiB；多阶梯实验的阶梯之间有 GC，不与一次写完的重复组混算。`--recovery` 在 WAL sync 等待后 SIGKILL 脚本自己启动的 ts-server，并重启校验。脚本有 8 GiB RSS 保护线，仅针对实验子进程，不是瞬时硬限制。

全量矩阵会更新导出的 CSV 和验证记录；原始基准结果可随时从 Git 查阅。`evidence.py` 需要矩阵产生的 heap profile 和制图虚拟环境：

```bash
python3 -m venv .venv
.venv/bin/pip install -r evidence/plot_dependencies.txt
python3 scripts/evidence.py
.venv/bin/python scripts/plot.py

go tool pprof -top -sample_index=alloc_space \
  -base runs/baseline1/profiles/baseline.heap \
  bin/ts-server-baseline runs/baseline1/profiles/gc_256.heap
```

`evidence.py` 中已归档的原始样本名称需对应生成；单跑一个新名称并不能重建全部证据。普通回归测试关闭内联以适配已有打桩测试，benchmark 保持正常优化。未运行整个仓库全量测试或 race suite。

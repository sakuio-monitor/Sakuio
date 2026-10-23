# 内存占用基线（Linux）/ Memory baseline

数据在 **Linux 容器**（OrbStack，Debian bookworm，aarch64，release 构建）中测得，单位 RSS。同一脚本可复现：`scripts/bench-100.sh`。

## 环境

| 项 | 值 |
|----|----|
| 运行环境 | OrbStack Linux 容器（Debian bookworm，aarch64），10 核 |
| 服务端 | `cargo build --release -p sakuio-server` |
| Agent | `cargo build --release`（Sakuio-agent） |
| 主库 / 指标库 | SQLite（colocated） |
| 负载 | 100 个真实 Agent 进程，采集 5s，`--disable-remote-control --disable-auto-update`；loopback |
| 测量 | `scripts/procmem.py`（Linux `/proc/<pid>/status` 的 VmRSS） |

## Agent 优化前后

本轮已落地的优化：

1. `CollectState` 不再 `System::new_all() + refresh_all()`，改为只刷新 CPU 列表（Linux 上不再常驻全部进程信息）。
2. macOS 进程数从 `ps -A` 子进程改为 `proc_listpids`。
3. macOS 网卡计数从 `netstat` 子进程改为 `sysinfo::Networks`（getifaddrs）。
4. macOS GPU 型号只探测一次并缓存（避免每个采样周期跑 `system_profiler`）。

| 场景 | 优化前（HEAD） | 优化后 |
|------|----------------|--------|
| 未连接（重连中） | — | 5.8 MB |
| 1 节点，60s | 8.7 MB | 8.5 MB |
| 1 节点，180s | 5.3 MB | **4.7 MB** |
| 100 节点，单个 Agent（60s） | 18.1 MB | **8.5 MB** |
| 100 节点，Agent 总计（60s） | 1821 MB | **854 MB** |
| 100 节点，Agent 总计（180s） | — | **446 MB** |

100 节点场景单 Agent 从 **18.1 MB 降到 8.5 MB（约 -53%）**；总内存从 **1.8 GB 降到 0.85 GB**。180s 后分配器回收/趋于稳定，总量可降到约 0.45 GB。

> Agent RSS 在前 1–3 分钟会先升后降（分配器保留页回收），对比时请使用相同采样时长。

## 服务端（Linux）

| 场景 | RSS | 线程 |
|------|-----|------|
| 0 节点（已安装） | 15.6 MB | 15 |
| 1 节点，60s | 16.2 MB | 17 |
| 100 节点，60s | 27.4 MB | 28 |
| 100 节点，180s | 27.8 MB | 27 |

100 节点服务端约 **26–28 MB**，相对空载约 **+11 MB**，约 **0.1 MB/节点**。

## 复现

```bash
# 构建（Linux 容器内示例）
cargo build --release -p sakuio-server
(cd ../Sakuio-agent && cargo build --release)

# 在 Linux 上运行
SAKUIO_SERVER_BIN=... SAKUIO_AGENT_BIN=... scripts/bench-100.sh 100 180
```

## 后续可继续压缩（未落地）

| 项 | 预估收益 | 代价 |
|----|----------|------|
| Tokio worker 线程从 10 降到 2，采集放 `spawn_blocking` | 1–2 MB | 需调整采集调用方式 |
| `MALLOC_ARENA_MAX=2` 或 mimalloc/jemalloc（低 decay） | 1–3 MB（高并发 glibc arena） | 构建/环境配置 |
| `lto=fat`、`codegen-units=1`、`panic=abort`、`strip` | 0.5–1.5 MB + 二进制更小 | 构建时间 |
| 连接数采样从 5s 放宽到 15–30s，或复用 netlink 句柄 | 0.5–1 MB | 图表精度下降 |
| Linux 完全去掉 sysinfo（直接读 `/proc`） | 0.5–1 MB | 工作量中等 |
| GPU 明细按 N 个周期采样并缓存静态信息 | 0–3 MB（GPU 机器） | GPU 曲线精度 |

目标：单 Agent 稳定在 **3.5–4.5 MB**，100 节点 Agent 总计 **400–500 MB**；当前优化后已接近该区间。

# CI/CD 流程（统一说明）

适用三个仓库：`Sakuio`（中控）、`Sakuio-agent`（探针）、`Sakuio-frontend`（前端）。
本文档是三仓库 CI/CD 的唯一权威说明，改动流程时同步更新此文件。

## 1. 仓库角色与依赖关系

```
Sakuio（主仓库）
  crates/sakuio-protocol   Agent 协议定义
  crates/sakuio-board      Agent 会话 / 传输抽象
  crates/sakuio-server     中控服务（依赖上面两个）
Sakuio-agent（探针）
  通过相对路径依赖 ../sakuio/crates/{sakuio-protocol, sakuio-board}
Sakuio-frontend（前端）
  bun workspace，纯前端，与上面两个无构建期依赖
```

由于 Agent 仓库使用 `../sakuio` 相对路径依赖，CI 中检出主仓库到
`$GITHUB_WORKSPACE/sakuio` 并软链到上一级目录（见 release.yml）。

## 2. Sakuio-agent：打 tag 自动构建发布

**触发**：推送 `v*` 格式的 tag（如 `v0.0.1`），或 GitHub Actions 页面手动
`workflow_dispatch`。

**流程**（`.github/workflows/release.yml`）：

1. `build` job 按矩阵交叉编译，产出裸二进制（无压缩包，统一命名风格）：

   | 目标 | 目标三元组 | 工具链 | 资产名 |
   |---|---|---|---|
   | linux x86_64 | `x86_64-unknown-linux-musl` | cargo-zigbuild | `sakuio-agent-linux-amd64` |
   | linux i686 | `i686-unknown-linux-musl` | cargo-zigbuild | `sakuio-agent-linux-386` |
   | linux aarch64 | `aarch64-unknown-linux-musl` | cargo-zigbuild | `sakuio-agent-linux-arm64` |
   | linux armv7 | `armv7-unknown-linux-musleabihf` | cargo-zigbuild | `sakuio-agent-linux-arm` |
   | linux loongarch64 | `loongarch64-unknown-linux-musl` | cargo-zigbuild | `sakuio-agent-linux-loong64` |
   | linux riscv64 | `riscv64gc-unknown-linux-musl` | cargo-zigbuild | `sakuio-agent-linux-riscv64` |
   | freebsd x86_64 | 原生 FreeBSD | FreeBSD VM + rust | `sakuio-agent-freebsd-amd64` |
   | darwin aarch64 | `aarch64-apple-darwin` | 原生 | `sakuio-agent-darwin-arm64` |
   | darwin x86_64 | `x86_64-apple-darwin` | 原生 | `sakuio-agent-darwin-amd64` |
   | windows x86_64 | `x86_64-pc-windows-msvc` | MSVC | `sakuio-agent-windows-amd64.exe` |
   | windows i686 | `i686-pc-windows-msvc` | MSVC | `sakuio-agent-windows-386.exe` |
   | windows aarch64 | `aarch64-pc-windows-msvc` | MSVC | `sakuio-agent-windows-arm64.exe` |

   说明：
   - Linux 目标全部使用 **musl 静态链接**，通过
     [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) 交叉编译，
     不依赖目标机的 glibc 版本；同一资产可在旧版和新版发行版直接运行。
     Release 构建会校验产物不是动态链接。zig 从官方归档直装
     `0.15.2`（实测可用；`setup-zig` 镜像不稳、builds 目录只留最新版）。
   - FreeBSD x86_64 通过 `vmactions/freebsd-vm` 在真实 FreeBSD 环境中构建，
     产物使用系统 libc（BSD 没有 musl）。
   - **MIPS 暂不提供**：Rust 上游已移除 mips 预编译 std
     （新版 rustup 对旧通道也拒绝下载），且 zig 不支持 MIPS。后续如需
     mips/mipsel，可评估 nightly `-Z build-std` 自行编标准库的方案。
   - macOS 在 `macos-14`（Apple Silicon）上原生编译并交叉出 x86_64。
   - Windows 在 `windows-2022` 上用 MSVC 交叉编译 386 / arm64。

2. `release` job：合并全部产物，发布到对应 tag 的 GitHub Release
   （自动生成 release notes）。

**发版步骤**：

```bash
# 1. 修改 Sakuio-agent/Cargo.toml 的 workspace.package.version
# 2. 提交推送
git commit -am "chore: bump version to x.y.z" && git push
# 3. 打 tag 触发构建
git tag vx.y.z && git push origin vx.y.z
```

**一键安装命令约定**（管理端「添加服务器」实时生成）：

```
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio-agent/main/install.sh \
  | sudo bash -s -- -e <端点> -t <token> [安装选项...]
```

`install.sh` 按 `sakuio-agent-<os>-<arch>` 命名从 Release 下载裸二进制，
支持 `--github-proxy` 加速、`--version` 锁定版本。

## 3. Sakuio（中控）

CI：`.github/workflows/ci.yml` 在 push main 和 pull request 时执行：

- `cargo test --workspace --all-targets`
- `cargo test -p sakuio-server ddns:: -- --nocapture`

DDNS / 授权 API 的驱动注册、Cloudflare Token/Global Key 配置解析、A/AAAA
地址选择、错误路径和数据库辅助函数均有单元测试。真实 Cloudflare 网络不在
CI 中访问。

### 发布与安装

推送 `v*` tag 或手动运行 `.github/workflows/release.yml`：

| 目标 | 资产名 |
|---|---|
| linux x86_64 | `sakuio-server-linux-amd64`（static musl） |
| linux i686 | `sakuio-server-linux-386`（static musl） |
| linux aarch64 | `sakuio-server-linux-arm64`（static musl） |
| linux armv7 | `sakuio-server-linux-arm`（static musl） |
| linux loongarch64 | `sakuio-server-linux-loong64`（static musl） |
| linux riscv64 | `sakuio-server-linux-riscv64`（static musl） |
| windows x86_64 | `sakuio-server-windows-amd64.exe` |
| windows i686 | `sakuio-server-windows-386.exe` |
| windows arm64 | `sakuio-server-windows-arm64.exe` |

同一 Release 还包含管理端构建产物和 `sakuio-theme-<tag>.zip`；服务端以
`embedded-admin` feature 嵌入管理端静态文件，安装脚本下载主题包到
`/opt/sakuio/data/theme/SakuioTheme`。

每个资产同时发布 `.sha256`；安装脚本会校验（Release 提供时）。

一键安装：

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio/main/install.sh | sudo bash
```

安装脚本注册 systemd / launchd 服务和 `/usr/local/bin/sakuio` 管理命令。
`sakuio` 提供启动、停止、重启、状态、实时日志、最近日志、更新、修改用户名、
修改密码、关闭 TOTP，以及 GitHub 加速地址输入。

运行时配置位于可执行文件同目录 `config.yaml`，权限 `0600`；只有选择 SQLite
作为主库或指标库时才创建 SQLite 文件。改动推送前必须本地编译通过。

## 4. Sakuio-frontend（前端）

当前无 CI：bun workspace，`bun run dev:admin` 本地验证；构建检查
（`bun run build` + 类型检查）接入 CI 时同样以 tag / main 分支推送触发，
流程更新时回写本文档。

## 5. 约定

- 版本号：三仓库独立版本，agent 从 `0.0.1` 起步，SemVer。
- tag 命名：`v<semver>`；仓库同名 tag 即 release。
- 任何 CI 产物与提交中不得包含：数据库文件、环境配置、密钥、
  竞品名称字符串（见各仓库 .gitignore）。

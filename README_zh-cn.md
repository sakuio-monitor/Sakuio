![Sakuio](https://socialify.git.ci/sakuio-monitor/Sakuio/image?forks=1&issues=1&language=1&name=1&owner=1&pulls=1&stargazers=1&theme=Light&logo=https%3A%2F%2Fraw.githubusercontent.com%2Fsakuio-monitor%2FSakuio%2Fmain%2Fassets%2Fsakuio-icon.svg)

[English](./README.md) | [简体中文](./README_zh-cn.md)

Sakuio 是一个轻量、低资源占用的自托管服务器探针与监控面板。它通过轻量 Agent 采集节点数据，在 Web 界面中提供实时状态、历史图表、远程工作台、告警通知与主题系统。

> [!IMPORTANT]
> Sakuio 具备服务器状态采集和远程管理能力。部署前请确认你拥有目标服务器的管理权限，并自行配置访问控制、TLS、账号权限和备份策略。因配置不当或未授权使用产生的风险与后果由部署者承担。

## 特性

- **轻量低占用**：服务端与 Agent 均使用 Rust，适合长期运行在各类服务器上
- **实时监控**：CPU、内存、Swap、磁盘、网络、负载、温度、连接数、进程数与 GPU
- **多种实时通道**：SSE、WebSocket、HTTP 轮询，可在主题配置中切换
- **历史图表**：支持负载、延迟、网络等指标的时间范围查询
- **远程工作台**：Web 终端、文件管理、在线编辑与远程命令执行
- **主题系统**：主题安装、配置、预览与切换
- **Komari 主题兼容**：现有 Komari 主题可以直接安装使用
- **多数据库**：SQLite、PostgreSQL、MySQL、MariaDB
- **通知与 DDNS**：离线、负载、流量、到期提醒与动态域名更新
- **响应式界面**：桌面端与移动端均可使用，支持亮色 / 暗色模式

## 项目组成

| 项目 | 说明 |
| --- | --- |
| [Sakuio](https://github.com/sakuio-monitor/Sakuio) | 中控服务、协议、数据库与 Web API |
| [Sakuio-agent](https://github.com/sakuio-monitor/Sakuio-agent) | 跨平台节点采集端 |
| [Sakuio-frontend](https://github.com/sakuio-monitor/Sakuio-frontend) | 管理后台与前台主题 |

## 快速开始

### 环境要求

- Rust stable
- Node.js 20+（仅前端开发）
- Bun 或 pnpm（前端包管理）

### 启动中控

```bash
git clone https://github.com/sakuio-monitor/Sakuio.git
cd Sakuio

cargo build --release -p sakuio-server
./target/release/sakuio-server
```

默认监听：

```text
http://0.0.0.0:8555
```

首次启动会在可执行文件同目录生成 `config.yaml`，写入默认监听地址；完成安装后，数据库连接信息也写入这个文件。只有选择 SQLite 作为主库或指标库时才会生成 SQLite 文件。

### 一键安装

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio/main/scripts/install.sh | sudo bash
```

安装脚本会依次询问：

- 监听端口，默认 `8555`
- 安装目录，默认 `/opt/sakuio`
- GitHub 加速站 URL，可留空

安装脚本还会下载并解压默认主题到 `/opt/sakuio/data/theme/SakuioTheme`，首次初始化后自动启用该主题。

然后 `sudo sakuio` 会打开数字菜单，可启动、停止、重启、查看日志、更新程序、修改用户名、修改密码、关闭 TOTP。

### 环境变量覆盖

```bash
SAKUIO_LISTEN=0.0.0.0:8555
SAKUIO_DATABASE_DSN=./data/sakuio.db
```

环境变量优先于 `config.yaml`。完整示例见 [`config.example.yaml`](config.example.yaml)。

只有同时满足以下三个条件，程序才不会进入初始化状态：

1. `config.yaml` 存在
2. 配置的主库可以连接
3. 主库中存在 `initialized = true`

`metric_database.mode: same` 表示指标表与主库共用同一个数据库，不是没有指标库。使用独立指标库时写 `mode: separate`，并在同一层级填写连接字段。数据库连接字段在 `config.yaml` 中拆开声明：`driver`、`host`、`port`、`database`、`user`、`password`、`path`。

配置的主库连接失败时，程序会进入初始化状态，允许重新配置数据库。

### 添加节点

在后台「服务器管理」中新增节点，将生成的安装命令复制到目标服务器执行：

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio-agent/main/install.sh | bash -s -- \
  --endpoint ws://<中控地址>:8555 \
  --token "<节点 Token>"
```

更多参数见 [Sakuio-agent](https://github.com/sakuio-monitor/Sakuio-agent)。

### 前端开发

```bash
git clone https://github.com/sakuio-monitor/Sakuio-frontend.git
cd Sakuio-frontend

bun install
bun run dev:admin
bun run dev:public
```

## 前端与主题

管理后台使用 React，前台默认主题使用 Vue 3 + Naive UI。

默认主题基于 [lyimoexiao/komari-theme-naive](https://github.com/lyimoexiao/komari-theme-naive) 修改而来。

工作台等部分前端代码基于 [komari-monitor/komari](https://github.com/komari-monitor/komari) 主仓库进行适配与重构。

前端保持 Komari 主题协议兼容，已有 Komari 主题可以无缝接入。主题加载、配置管理和实时数据通道经过针对性优化，注重资源占用与响应速度。

## 项目结构

```text
Sakuio/
├── crates/
│   ├── sakuio-server/       # 中控 HTTP / WebSocket / SSE 服务
│   ├── sakuio-db/           # 数据库与仓储
│   ├── sakuio-core/         # 节点状态与核心逻辑
│   ├── sakuio-protocol/     # SakuLink 协议
│   ├── sakuio-board/        # 节点控制面
│   ├── sakuio-metric/       # 指标存储
│   ├── sakuio-notify/       # 通知
│   └── sakuio-transport-ws/ # WebSocket 传输
├── data/                    # 运行时数据（不入库）
├── docs/                    # 设计与运维文档
├── assets/                  # README / 品牌资源
└── scripts/                 # 开发与测试脚本
```

## 文档

- [主题开发](docs/THEMES.md)
- [仪表盘](docs/DASHBOARD.md)
- [通知与告警](docs/AUDIT.md)
- [DDNS](docs/DDNS.md)
- [测试](docs/TESTING.md)
- [CI/CD](docs/CICD.md)

## Contributors

感谢每一位为 Sakuio 提交代码、主题、文档、翻译、问题反馈或建议的贡献者，你们的参与让项目不断完善。

<a href="https://github.com/sakuio-monitor/Sakuio/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=sakuio-monitor/Sakuio" alt="Sakuio contributors">
</a>

## 许可证

本项目使用 [MIT License](LICENSE)。

## 致谢

- 默认主题基于 [komari-theme-naive](https://github.com/lyimoexiao/komari-theme-naive)
- 工作台等部分前端代码基于 [Komari 主仓库](https://github.com/komari-monitor/komari)
- 感谢所有为 Sakuio 提交代码、反馈问题和提供建议的贡献者
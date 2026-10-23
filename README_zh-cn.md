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
http://0.0.0.0:8080
```

首次启动会进入安装向导，完成数据库与管理员账号初始化。

常用环境变量：

```bash
SAKUIO_LISTEN=0.0.0.0:8080
SAKUIO_DATABASE_DSN=./data/sakuio.db
```

### 添加节点

在后台「服务器管理」中新增节点，将生成的安装命令复制到目标服务器执行：

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio-agent/main/install.sh | bash -s -- \
  --endpoint ws://<中控地址>:8080 \
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
  <img src="https://contributors-img.web.app/image?repo=sakuio-monitor/Sakuio" alt="Sakuio contributors" width="600">
</a>

## 许可证

本项目使用 [MIT License](LICENSE)。

## 致谢

- 默认主题基于 [komari-theme-naive](https://github.com/lyimoexiao/komari-theme-naive)
- 工作台等部分前端代码基于 [Komari 主仓库](https://github.com/komari-monitor/komari)
- 感谢所有为 Sakuio 提交代码、反馈问题和提供建议的贡献者
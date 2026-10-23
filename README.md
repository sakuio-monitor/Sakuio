![Sakuio](https://socialify.git.ci/sakuio-monitor/Sakuio/image?forks=1&issues=1&language=1&name=1&owner=1&pulls=1&stargazers=1&theme=Light&logo=https%3A%2F%2Fraw.githubusercontent.com%2Fsakuio-monitor%2FSakuio%2Fmain%2Fassets%2Fsakuio-icon.svg)

[English](./README.md) | [简体中文](./README_zh-cn.md)

Sakuio is a lightweight, low-resource self-hosted server monitoring and probe platform. A small agent collects node metrics, while the web interface provides live status, historical charts, a remote workbench, alerts, and a theme system.

> [!IMPORTANT]
> Sakuio can collect server metrics and perform remote management. Before deployment, make sure you are authorized to manage every target server, and configure access control, TLS, account permissions, and backups appropriately. Risks and consequences from misconfiguration or unauthorized use are the deployer's responsibility.

## Features

- **Lightweight**: both the server and agent are written in Rust and are designed for long-running deployments
- **Live monitoring**: CPU, memory, swap, disk, network, load, temperature, connections, processes, and GPU
- **Multiple live transports**: SSE, WebSocket, and HTTP polling, selectable in the theme configuration
- **Historical charts**: time-range queries for load, latency, network, and other metrics
- **Remote workbench**: web terminal, file manager, online editing, and remote command execution
- **Theme system**: install, configure, preview, and switch themes
- **Komari theme compatibility**: existing Komari themes can be installed and used directly
- **Multiple databases**: SQLite, PostgreSQL, MySQL, and MariaDB
- **Notifications and DDNS**: offline, load, traffic, and expiry alerts, plus dynamic DNS updates
- **Responsive UI**: desktop and mobile layouts with light and dark modes

## Repositories

| Repository | Description |
| --- | --- |
| [Sakuio](https://github.com/sakuio-monitor/Sakuio) | Server, protocol, database, and Web API |
| [Sakuio-agent](https://github.com/sakuio-monitor/Sakuio-agent) | Cross-platform node agent |
| [Sakuio-frontend](https://github.com/sakuio-monitor/Sakuio-frontend) | Admin panel and public themes |

## Quick Start

### Requirements

- Rust stable
- Node.js 20+ (frontend development only)
- Bun or pnpm (frontend package manager)

### Start the server

```bash
git clone https://github.com/sakuio-monitor/Sakuio.git
cd Sakuio

cargo build --release -p sakuio-server
./target/release/sakuio-server
```

Default address:

```text
http://0.0.0.0:8555
```

On first launch Sakuio creates `config.yaml` next to the executable with a default listen address. The setup wizard then stores the selected database connection there. A SQLite file is only created when SQLite is selected as the main or metric database.

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio/main/install.sh | sudo bash
```

The installer asks for:

- listen port (default `8555`)
- install directory (default `/opt/sakuio`)
- optional GitHub proxy URL

The installer also downloads and unpacks the default theme into `/opt/sakuio/data/theme/SakuioTheme`, and the first setup activates it.

Then `sudo sakuio` opens a menu for start/stop/restart, logs, updates, username, password, and TOTP reset.

### Environment overrides

```bash
SAKUIO_LISTEN=0.0.0.0:8555
SAKUIO_DATABASE_DSN=./data/sakuio.db
```

Environment variables override `config.yaml`. See [`config.example.yaml`](config.example.yaml) for the full file.

A fresh install is only considered complete when all of these are true:

1. `config.yaml` exists
2. the configured main database is reachable
3. the database contains `initialized = true`

`metric_database.mode: same` means metrics are stored in the main database. Use `mode: separate` plus a structured connection when using an independent metric database. Database connection fields are declared separately in `config.yaml` (`driver`, `host`, `port`, `database`, `user`, `password`, `path`).

If the configured database cannot be reached, Sakuio starts in setup mode so the connection can be repaired.

### Add a node

Create a node in **Servers**, then run the generated installation command on the target machine:

```bash
curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio-agent/main/install.sh | bash -s -- \
  --endpoint ws://<sakuio-host>:8555 \
  --token "<node-token>"
```

See [Sakuio-agent](https://github.com/sakuio-monitor/Sakuio-agent) for all options.

### Frontend development

```bash
git clone https://github.com/sakuio-monitor/Sakuio-frontend.git
cd Sakuio-frontend

bun install
bun run dev:admin
bun run dev:public
```

## Frontend and Themes

The admin panel is built with React. The default public theme uses Vue 3 and Naive UI.

The default theme is based on [lyimoexiao/komari-theme-naive](https://github.com/lyimoexiao/komari-theme-naive).

The workbench and some other frontend code were adapted and refactored from the [Komari main repository](https://github.com/komari-monitor/komari).

The frontend keeps the Komari theme protocol compatible, so existing Komari themes can be installed without changes. Theme loading, configuration, and live data transport have been tuned for low resource usage and responsive updates.

## Project Structure

```text
Sakuio/
├── crates/
│   ├── sakuio-server/       # HTTP / WebSocket / SSE server
│   ├── sakuio-db/           # Database and repositories
│   ├── sakuio-core/         # Node state and core logic
│   ├── sakuio-protocol/     # SakuLink protocol
│   ├── sakuio-board/        # Node control plane
│   ├── sakuio-metric/       # Metric storage
│   ├── sakuio-notify/       # Notifications
│   └── sakuio-transport-ws/ # WebSocket transport
├── data/                    # Runtime data (ignored)
├── docs/                    # Design and operations docs
├── assets/                  # README / branding assets
└── scripts/                 # Development and test scripts
```

## Documentation

- [Theme development](docs/THEMES.md)
- [Dashboard](docs/DASHBOARD.md)
- [Notifications and audit](docs/AUDIT.md)
- [DDNS](docs/DDNS.md)
- [Testing](docs/TESTING.md)
- [CI/CD](docs/CICD.md)

## Contributors

Thanks to everyone who has contributed code, themes, documentation, translations, bug reports, or feedback. Your work helps Sakuio keep improving.

<a href="https://github.com/sakuio-monitor/Sakuio/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=sakuio-monitor/Sakuio" alt="Sakuio contributors">
</a>

## License

Released under the [MIT License](LICENSE).

## Acknowledgements

- The default theme is based on [komari-theme-naive](https://github.com/lyimoexiao/komari-theme-naive)
- The workbench and some frontend code are based on the [Komari main repository](https://github.com/komari-monitor/komari)
- Thanks to everyone who contributed code, feedback, and suggestions to Sakuio

# 主题系统 / Theme system

Sakuio 的用户前台主题与 [Komari](https://github.com/komari-monitor/komari) 兼容：主题包根目录包含 `komari-theme.json`，静态资源位于 `dist/`。主题可上传、从 Komari 主题市场安装、应用/切换、更新与删除，**全部无需重启进程**。

## 目录布局

```
data/theme/
├── AdhesiveNote/
│   ├── komari-theme.json
│   ├── .sakuio-source.json   # 市场来源/版本（服务端内部）
│   └── dist/
│       ├── index.html
│       └── assets/...
└── ...
```

当前主题保存在 `system_settings` 的 `theme` 键；缺失或已删除时回退到内置 `default` 占位主题。

## 模块

| 文件 | 职责 |
|------|------|
| `crates/sakuio-server/src/theme/manifest.rs` | 解析/校验 `komari-theme.json`（支持字符串或多语言对象） |
| `.../theme/archive.rs` | ZIP 安全解压：路径穿越、符号链接、文件数/单文件/总大小限制，支持根目录或单层目录布局 |
| `.../theme/store.rs` | 文件系统仓库：列表、安装（原子替换）、读取、删除、来源元数据 |
| `.../theme/market.rs` | Komari 市场目录（默认官方 `v1.json`）、下载、SHA-256 校验、GitHub Release 更新 |
| `.../theme/mod.rs` | `ThemeService`：激活主题、热切换、站点字段注入、静态文件解析 |
| `crates/sakuio-server/src/theme_api.rs` | 管理端 HTTP API |

## HTTP API（均需管理员鉴权）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/admin/themes` | 已安装主题、当前主题、市场地址 |
| POST | `/api/admin/themes/upload` | 请求体为原始 ZIP（上限 128 MB） |
| GET | `/api/admin/themes/:short/preview` | 已安装主题预览图（管理端鉴权，供列表缩略图） |
| GET | `/api/admin/themes/:short/config` | 读取主题声明的托管配置项与当前值 |
| PUT | `/api/admin/themes/:short/config` | 校验并保存配置，立即应用（无需重启） |
| POST | `/api/admin/themes/:short/apply` | 立即应用（写设置 + 内存切换） |
| DELETE | `/api/admin/themes/:short` | 删除（禁止删除当前主题/`default`） |
| POST | `/api/admin/themes/:short/update` | 启动异步更新任务，返回 `{job_id}`；市场主题按版本，上传主题按清单 `url` |
| GET | `/api/admin/themes/market` | 市场列表；`?refresh=1` 强制刷新 |
| POST | `/api/admin/themes/market/:short/install` | 启动异步下载/安装任务，返回 `{job_id}`（202） |
| GET | `/api/admin/themes/jobs/:id` | 任务快照（轮询回退） |
| GET | `/api/admin/themes/jobs/:id/events` | SSE 实时进度（阶段、已下载字节、总字节、速度、百分比） |

前台由路由器 fallback 提供：

- `/` 与无扩展名路径 → 当前主题 `dist/index.html`（SPA 回退）；
- `/assets/...` 等 → 当前主题 `dist/...`；
- `/themes/:id/*` → 指定主题根目录文件（预览图等）。

## 异步下载与进度

市场安装/更新不会阻塞 HTTP 请求：接口立即返回 `202 + job_id`，下载在后台任务中进行。不同主题可同时下载（服务端最多 3 个并发，超出的任务显示 `queued`），管理端为每个主题卡片单独订阅进度：

```
GET /api/admin/themes/jobs/:job_id/events
event: progress
data: {"phase":"downloading","status":"running","downloaded":1487787,"total":4300209,"speed_bps":5611921,"percent":34.6,...}
```

阶段依次为 `queued → resolving → downloading → retrying? → installing → done`；失败时 `status=error` 并带可读 `message`（例如 SHA-256 不一致会同时给出期望值与实际值，并提示市场元数据可能过期）。网络类错误自动重试一次；SSE 断开时前端自动改用 `/jobs/:id` 快照轮询，直到任务结束。

## 主题配置（Komari managed configuration）

主题可在 `komari-theme.json` 中声明 `configuration`，Sakuio 会像 Komari 一样读取、编辑、存储并应用：

```json
{
  "configuration": {
    "type": "managed",
    "name": { "en": "Theme Settings", "zh-CN": "主题设置" },
    "data": [
      { "name": "General", "type": "title" },
      { "key": "enabled", "name": "Enabled", "type": "switch", "default": true },
      { "key": "mode", "name": "Mode", "type": "select", "options": "Fast,Slow", "default": "Slow" },
      { "key": "label", "name": "Label", "type": "string", "required": true },
      { "key": "html", "name": "HTML", "type": "richtext" },
      { "key": "nodes", "name": "Nodes", "type": "nodes" },
      { "key": "tasks", "name": "Tasks", "type": "pingtasks" }
    ]
  }
}
```

支持的类型：`title`、`textbox`、`string`、`number`、`select`、`switch`、`richtext`、`nodes`、`pingtasks`。`nodes`/`pingtasks` 在管理端是多选列表，存储为 Komari 兼容的 JSON 字符串，公开输出时转换为数组。

- 默认值按 `default` 或类型默认值补齐；
- `required`、`select.options` 会在保存时校验；
- 配置保存在 `system_settings` 的 `theme_config:<short>`，重装/更新主题不会丢失；
- 当前主题的配置通过 `theme_settings` 出现在 `/api/public`（及 `common:getPublicInfo`）中，主题 JS 读取后立即生效，无需重启；
- 管理端为独立页面 `/themes/:short/settings`，入口在“个性化”分组下，菜单/页面标题使用 `configuration.name`（缺失时回退为主题名 + Theme Management），与主题管理、主题市场同级；不再使用弹窗；
- 非 `managed` 类型（`raw`/`redirect`）不提供托管配置编辑。

## 安全与限制

- ZIP：最多 10,000 个文件、单文件 128 MB、解压总量 512 MB；拒绝绝对路径、`..`、反斜杠路径和符号链接；先解压到 `.staging-*`，校验后再原子替换。
- 网络：仅允许无凭据的 HTTP(S)，拒绝 localhost/字面私网 IP；市场包默认 100 MB 上限；有 `sha256` 时必须匹配。
- 静态文件读取经过 canonicalize + 根目录前缀校验，隐藏文件不可通过 `/themes/...` 访问。
- `default` 为内置占位主题，不可删除/覆盖。

## 站点字段注入

与 Komari 一致，服务端会把 `dist/index.html` 中的：

- `<title>Komari Monitor</title>` → 站点名称；
- `A simple server monitor tool.` → 站点描述；
- `</head>` / `</body>` 前插入自定义 head/body。

这样第三方 Komari 主题无需修改即可显示站点名称。

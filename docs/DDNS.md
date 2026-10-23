# 授权 API 与 DDNS

## 模块化驱动

`crates/sakuio-server/src/ddns` 使用 trait + `inventory` 自注册：

- `DdnsDriver` 定义驱动标识、配置字段、Token 校验、DNS 更新。
- 驱动通过 `inventory::submit!` 注册。
- 删除一个驱动模块和对应的 `mod` 声明即可移除该驱动，不需要修改中心匹配表。
- 当前内置驱动：Cloudflare、阿里云、华为云、腾讯云、百度云、火山引擎、GoDaddy、Spaceship、Namecheap、西部数码、AWS Route 53、Azure DNS。

## 数据表

- `auth_providers`：名称、驱动、加密前的配置 JSON、最后验证时间。
- `ddns_records`：授权、服务器、可管理根域名、记录名、A/AAAA、代理、TTL、缓存 Zone/Record ID、最后 IP、错误。
- 页面只输入最终解析域名；后端自动识别 `com.cn`、`co.uk`、`edu.cn` 等复合后缀。
- 支持 A、AAAA、A + AAAA；同时解析会创建两条实际记录。
- 自动同步始终启用，不提供关闭开关；服务器 IP 变化后自动更新。
- 页面只输入最终解析域名，后端自动识别 `com.cn`、`co.uk`、`edu.cn` 等复合后缀。
- 支持 A、AAAA、A + AAAA 同时解析；同时解析会创建两条独立记录。

## 流程

1. 添加授权时调用驱动 `validate`，Cloudflare 使用 `/user/tokens/verify`。
2. 验证失败不写入数据库。
3. 添加/修改 DDNS 记录时立即写入一次 DNS。
4. 后台每 30 秒读取服务器当前 IPv4/IPv6，和记录 `last_ip` 比较；变化时更新 DNS。
5. Cloudflare 支持 API Token 和 Global API Key + 账户邮箱两种授权。
5. 多个 DDNS 记录可以指向同一台或不同服务器。
6. 失败写入 `last_error`，下一次后台周期重试。

## API

```text
GET  /api/admin/auth/drivers
GET  /api/admin/auth/providers
POST /api/admin/auth/providers
PUT  /api/admin/auth/providers/:id
DELETE /api/admin/auth/providers/:id

GET  /api/admin/ddns/clients
GET  /api/admin/ddns
POST /api/admin/ddns
PUT  /api/admin/ddns/:id
DELETE /api/admin/ddns/:id
POST /api/admin/ddns/:id/sync
```

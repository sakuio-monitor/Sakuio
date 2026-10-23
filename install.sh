#!/usr/bin/env bash
# Sakuio server one-line installer and service manager installer.
#
# curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio/main/install.sh \
#   | sudo bash -s -- [--listen 0.0.0.0:8555] [--github-proxy URL]
#
# Options:
#   --version V          Install a specific version (default: latest release)
#   --listen ADDR        Listen address (default: 0.0.0.0:8555)
#   --install-dir DIR    Install directory (default: /opt/sakuio)
#   --service-name NAME  systemd/launchd service name (default: sakuio)
#   --github-proxy URL   GitHub download mirror prefix
#   --uninstall          Remove the service and binary
#   --purge              With --uninstall, also remove application data
#   -h, --help           Show this help

set -euo pipefail

REPO="sakuio-monitor/Sakuio"
VERSION=""
LISTEN="0.0.0.0:8555"
INSTALL_DIR="/opt/sakuio"
SERVICE_NAME="sakuio"
GITHUB_PROXY="${GITHUB_PROXY:-}"
UNINSTALL=0
PURGE=0
INTERACTIVE=0
[[ $# -eq 0 ]] && INTERACTIVE=1

usage() { sed -n '2,22p' "$0"; exit 0; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --listen) LISTEN="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --github-proxy) GITHUB_PROXY="$2"; shift 2 ;;
    --uninstall) UNINSTALL=1; shift ;;
    --purge) PURGE=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: run this installer as root (use sudo)" >&2
  exit 1
fi

# Short command (`curl ... | sudo bash`) asks only for the essentials.
# When stdin is the script pipe, read prompts from /dev/tty.
ask_tty() {
  local prompt="$1" default="$2" answer=""
  if [[ -r /dev/tty ]]; then
    read -r -p "$prompt" answer < /dev/tty || true
  fi
  printf '%s' "${answer:-$default}"
}

if [[ "$INTERACTIVE" -eq 1 ]]; then
  port_input="$(ask_tty "监听端口 [8555]: " "8555")"
  if [[ ! "$port_input" =~ ^[0-9]+$ ]] || (( port_input < 1 || port_input > 65535 )); then
    echo "error: invalid port: $port_input" >&2
    exit 1
  fi
  LISTEN="0.0.0.0:${port_input}"

  dir_input="$(ask_tty "安装目录 [/opt/sakuio]: " "/opt/sakuio")"
  INSTALL_DIR="${dir_input:-/opt/sakuio}"

  proxy_input="$(ask_tty "GitHub 加速站 URL（留空使用官方）: " "")"
  [[ -n "$proxy_input" ]] && GITHUB_PROXY="$proxy_input"
fi

# Accept accelerators such as https://ghfast.top and expand them to the
# GitHub-prefix form the proxy expects. A full prefix is used as-is.
github_base() {
  local proxy="${1%/}"
  if [[ -z "$proxy" || "$proxy" == "https://github.com" ]]; then
    printf '%s' "https://github.com"
  elif [[ "$proxy" == *"/https://github.com" || "$proxy" == *"/http://github.com" ]]; then
    printf '%s' "$proxy"
  else
    printf '%s' "$proxy/https://github.com"
  fi
}

GITHUB_BASE="$(github_base "$GITHUB_PROXY")"

is_darwin=0
[[ "$(uname -s)" == "Darwin" ]] && is_darwin=1

service_stop() {
  if [[ "$is_darwin" -eq 1 ]]; then
    launchctl bootout "system/$SERVICE_NAME" 2>/dev/null || true
    launchctl unload "/Library/LaunchDaemons/$SERVICE_NAME.plist" 2>/dev/null || true
  elif command -v systemctl >/dev/null 2>&1; then
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
  fi
}

if [[ "$UNINSTALL" -eq 1 ]]; then
  service_stop
  if [[ "$is_darwin" -eq 1 ]]; then
    rm -f "/Library/LaunchDaemons/$SERVICE_NAME.plist"
  else
    rm -f "/etc/systemd/system/$SERVICE_NAME.service"
    systemctl daemon-reload 2>/dev/null || true
  fi
  rm -f /usr/local/bin/sakuio
  rm -rf "$INSTALL_DIR"
  if [[ "$PURGE" -eq 1 ]]; then
    rm -rf "${INSTALL_DIR}.data"
  fi
  echo "Sakuio server uninstalled."
  exit 0
fi

# ---- platform detection -----------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "error: unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
  Linux) OS="linux" ;;
  Darwin) OS="darwin" ;;
  *) echo "error: unsupported OS: $OS" >&2; exit 1 ;;
esac

# ---- resolve version --------------------------------------------------------
if [[ -z "$VERSION" ]]; then
  LATEST_URL="${GITHUB_BASE}/${REPO}/releases/latest"
  if command -v curl >/dev/null 2>&1; then
    VERSION="$(curl -fsSL -o /dev/null -w '%{url_effective}' "$LATEST_URL" | sed 's|.*/tag/v||' | sed 's|/$||')"
  else
    VERSION="$(wget -qO- --max-redirs=5 "$LATEST_URL" 2>/dev/null | sed 's|.*/tag/v||' | sed 's|/$||' || true)"
  fi
  if [[ -z "$VERSION" || "$VERSION" == *"releases/latest"* ]]; then
    echo "error: failed to resolve latest version; pass --version" >&2
    exit 1
  fi
fi

ASSET="sakuio-server-${OS}-${ARCH}"
BASE="${GITHUB_BASE}/${REPO}/releases/download/v${VERSION}"
DOWNLOAD_URL="${BASE}/${ASSET}"

echo "==> installing Sakuio server v${VERSION} (${OS}/${ARCH})"
echo "==> downloading ${DOWNLOAD_URL}"

mkdir -p "$INSTALL_DIR/data"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
if command -v curl >/dev/null 2>&1; then
  curl -fL --retry 3 --retry-delay 2 -o "$TMP" "$DOWNLOAD_URL"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$TMP" "$DOWNLOAD_URL"
else
  echo "error: curl or wget is required" >&2
  exit 1
fi
chmod +x "$TMP"
# Verify the release checksum when the release publishes one.
if command -v curl >/dev/null 2>&1 && curl -fsSL --retry 2 -o "$TMP.sha256" "${DOWNLOAD_URL}.sha256" 2>/dev/null; then
  expected="$(awk '{print $1}' "$TMP.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$TMP" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$TMP" | awk '{print $1}')"
  fi
  if [[ -n "$expected" && "$expected" != "$actual" ]]; then
    echo "error: checksum mismatch for ${ASSET}" >&2
    rm -f "$TMP" "$TMP.sha256"
    exit 1
  fi
fi
install -m 755 "$TMP" "$INSTALL_DIR/sakuio-server"

# config.yaml is the source of truth. Write it before the first service start
# so a custom --listen value is persisted, not hidden in the service unit.
if [[ ! -f "$INSTALL_DIR/config.yaml" ]]; then
  umask 077
  cat > "$INSTALL_DIR/config.yaml" <<CONFIGEOF
listen: $LISTEN
database:
  driver: unconfigured
  max_connections: 16
metric_database:
  mode: same
CONFIGEOF
  chmod 600 "$INSTALL_DIR/config.yaml"
fi

# ---- default theme ---------------------------------------------------------
# The public theme is published as a separate release asset. It is unpacked
# before the server starts so the first initialization can activate it.
THEME_ASSET="frontend.zip"
THEME_URL="${BASE}/${THEME_ASSET}"
THEME_DIR="$INSTALL_DIR/data/theme/SakuioTheme"
THEME_TMP="$(mktemp)"
if command -v curl >/dev/null 2>&1; then
  if curl -fL --retry 3 -o "$THEME_TMP" "$THEME_URL" 2>/dev/null; then
    mkdir -p "$THEME_DIR"
    if command -v unzip >/dev/null 2>&1; then
      unzip -q -o "$THEME_TMP" -d "$THEME_DIR"
    elif command -v python3 >/dev/null 2>&1; then
      python3 -m zipfile -e "$THEME_TMP" "$THEME_DIR"
    else
      echo "warning: theme downloaded but unzip/python3 is unavailable" >&2
    fi
    if [[ -f "$THEME_DIR/sakuio-theme.json" || -f "$THEME_DIR/komari-theme.json" ]]; then
      echo "==> installed default theme SakuioTheme"
    else
      echo "warning: theme archive has no manifest" >&2
    fi
  else
    echo "warning: default theme package not found; server will start without it" >&2
  fi
else
  echo "warning: curl is unavailable; skipped default theme" >&2
fi
rm -f "$THEME_TMP"

# ---- service definition -----------------------------------------------------
if [[ "$is_darwin" -eq 1 ]]; then
  PLIST="/Library/LaunchDaemons/$SERVICE_NAME.plist"
  cat > "$PLIST" <<PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$SERVICE_NAME</string>
  <key>ProgramArguments</key><array><string>$INSTALL_DIR/sakuio-server</string></array>
  <key>WorkingDirectory</key><string>$INSTALL_DIR</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$INSTALL_DIR/data/sakuio.log</string>
  <key>StandardErrorPath</key><string>$INSTALL_DIR/data/sakuio.log</string>
</dict>
</plist>
PLISTEOF
  chmod 644 "$PLIST"
  launchctl bootout "system/$SERVICE_NAME" 2>/dev/null || true
  launchctl bootstrap system "$PLIST"
  launchctl enable "system/$SERVICE_NAME"
  launchctl kickstart -k "system/$SERVICE_NAME"
else
  cat > "/etc/systemd/system/$SERVICE_NAME.service" <<UNITEOF
[Unit]
Description=Sakuio server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/sakuio-server
Restart=on-failure
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
UNITEOF
  systemctl daemon-reload
  systemctl enable --now "$SERVICE_NAME"
fi

# ---- install the `sakuio` management command --------------------------------
cat > "$INSTALL_DIR/sakuio" <<CLIEOF
#!/usr/bin/env bash
set -uo pipefail

INSTALL_DIR="$INSTALL_DIR"
SERVICE_NAME="$SERVICE_NAME"
BIN="\$INSTALL_DIR/sakuio-server"
cd "\$INSTALL_DIR" || exit 1
github_base() {
  local proxy="\${1%/}"
  if [[ -z "\$proxy" || "\$proxy" == "https://github.com" ]]; then
    printf '%s' "https://github.com"
  elif [[ "\$proxy" == *"/https://github.com" || "\$proxy" == *"/http://github.com" ]]; then
    printf '%s' "\$proxy"
  else
    printf '%s' "\$proxy/https://github.com"
  fi
}
IS_DARWIN=0
[[ "\$(uname -s)" == "Darwin" ]] && IS_DARWIN=1

require_root() {
  if [[ "\$(id -u)" -ne 0 ]]; then
    echo "需要 root 权限，请使用 sudo sakuio" >&2
    exit 1
  fi
}

svc() {
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    launchctl "\$@"
  else
    systemctl "\$@"
  fi
}

svc_start() { require_root; svc start "\$SERVICE_NAME"; }
svc_stop() { require_root; svc stop "\$SERVICE_NAME"; }
svc_restart() { require_root; svc restart "\$SERVICE_NAME"; }
svc_status() {
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    launchctl print "system/\$SERVICE_NAME" 2>/dev/null | sed -n '1,20p' || echo "未运行"
  else
    systemctl status "\$SERVICE_NAME" --no-pager
  fi
}
svc_logs() {
  require_root
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    exec tail -f "\$INSTALL_DIR/data/sakuio.log"
  else
    exec journalctl -u "\$SERVICE_NAME" -f --no-pager
  fi
}
svc_logs100() {
  require_root
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    tail -n 100 "\$INSTALL_DIR/data/sakuio.log"
  else
    journalctl -u "\$SERVICE_NAME" -n 100 --no-pager
  fi
}

latest_version() {
  local base
  base="\$(github_base "\${GITHUB_PROXY:-}")"
  local latest_url="\${base}/sakuio-monitor/Sakuio/releases/latest"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o /dev/null -w '%{url_effective}' "\$latest_url" | sed 's|.*/tag/v||' | sed 's|/\$||'
  else
    wget -qO- --max-redirs=5 "\$latest_url" 2>/dev/null | sed 's|.*/tag/v||'
  fi
}

do_update() {
  require_root
  local proxy="\${GITHUB_PROXY:-}"
  read -r -p "GitHub 加速地址（留空使用官方 GitHub）: " input_proxy
  [[ -n "\$input_proxy" ]] && proxy="\$input_proxy"
  local version
  read -r -p "目标版本（留空安装最新）: " version
  if [[ -z "\$version" ]]; then
    GITHUB_PROXY="\$proxy" version="\$(latest_version)"
  fi
  if [[ -z "\$version" || "\$version" == *"releases/latest"* ]]; then
    echo "无法获取版本号" >&2
    return 1
  fi
  local os arch asset url tmp
  case "\$(uname -s)" in Linux) os=linux ;; Darwin) os=darwin ;; *) echo "不支持的系统" >&2; return 1 ;; esac
  case "\$(uname -m)" in x86_64|amd64) arch=amd64 ;; aarch64|arm64) arch=arm64 ;; *) echo "不支持的架构" >&2; return 1 ;; esac
  asset="sakuio-server-\${os}-\${arch}"
  local base
  base="\$(github_base "\${proxy:-}")"
  url="\${base}/sakuio-monitor/Sakuio/releases/download/v\${version}/\${asset}"
  tmp="\$(mktemp)"
  echo "==> 下载 \$url"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 -o "\$tmp" "\$url" || { rm -f "\$tmp"; return 1; }
  else
    wget -qO "\$tmp" "\$url" || { rm -f "\$tmp"; return 1; }
  fi
  chmod +x "\$tmp"
  svc_stop || true
  cp -p "\$BIN" "\$BIN.bak" 2>/dev/null || true
  if install -m 755 "\$tmp" "\$BIN.new" && mv "\$BIN.new" "\$BIN"; then
    rm -f "\$tmp"
    svc_start || true
    echo "已更新到 v\${version}"
  else
    rm -f "\$tmp"
    [[ -f "\$BIN.bak" ]] && mv "\$BIN.bak" "\$BIN"
    svc_start || true
    echo "更新失败，已回滚" >&2
    return 1
  fi
}

admin_user() {
  local user
  read -r -p "管理员用户名 [admin]: " user
  echo "\${user:-admin}"
}

do_username() {
  local old new
  old="\$(admin_user)"
  read -r -p "新用户名: " new
  [[ -z "\$new" ]] && { echo "用户名不能为空"; return 1; }
  "\$BIN" admin set-username "\$new" --username "\$old"
}

do_password() {
  local user pass pass2
  user="\$(admin_user)"
  read -r -s -p "新密码（至少 8 位）: " pass; echo
  read -r -s -p "再次输入新密码: " pass2; echo
  [[ "\$pass" != "\$pass2" ]] && { echo "两次密码不一致"; return 1; }
  printf '%s\n' "\$pass" | "\$BIN" admin set-password --username "\$user"
}

do_disable_totp() {
  local user
  user="\$(admin_user)"
  "\$BIN" admin disable-totp --username "\$user"
}

menu() {
  cat <<MENU
Sakuio 管理菜单
  1) 启动服务
  2) 停止服务
  3) 重启服务
  4) 查看服务状态
  5) 查看实时日志
  6) 查看最近 100 条日志
  7) 更新程序
  8) 修改用户名
  9) 修改密码
 10) 关闭 TOTP
 11) 查看管理员账号
  0) 退出
MENU
}

if [[ \$# -gt 0 ]]; then
  case "\$1" in
    start) svc_start ;;
    stop) svc_stop ;;
    restart) svc_restart ;;
    status) svc_status ;;
    logs) svc_logs ;;
    logs100) svc_logs100 ;;
    update) do_update ;;
    set-username) shift; "\$BIN" admin set-username "\$@" ;;
    set-password) shift; "\$BIN" admin set-password "\$@" ;;
    disable-totp) shift; "\$BIN" admin disable-totp "\$@" ;;
    list-admins) "\$BIN" admin list ;;
    *) echo "用法: sakuio [start|stop|restart|status|logs|logs100|update|set-username|set-password|disable-totp|list-admins]"; exit 1 ;;
  esac
  exit \$?
fi

while true; do
  menu
  read -r -p "请选择 [0-11]: " choice
  case "\$choice" in
    1) svc_start ;;
    2) svc_stop ;;
    3) svc_restart ;;
    4) svc_status ;;
    5) svc_logs ;;
    6) svc_logs100 ;;
    7) do_update ;;
    8) do_username ;;
    9) do_password ;;
    10) do_disable_totp ;;
    11) "\$BIN" admin list ;;
    0) exit 0 ;;
    *) echo "无效选项" ;;
  esac
  echo
done
CLIEOF
chmod 755 "$INSTALL_DIR/sakuio"
ln -sfn "$INSTALL_DIR/sakuio" /usr/local/bin/sakuio

echo
echo "Sakuio 安装完成。"
echo "监听地址: $LISTEN"
echo "管理命令: sudo sakuio"
echo "打开 http://<服务器地址>:$(echo "$LISTEN" | sed 's/.*://') 完成初始化。"

#!/usr/bin/env bash
# Sakuio server one-line installer and service manager installer.
#
# curl -fsSL https://raw.githubusercontent.com/sakuio-monitor/Sakuio/main/scripts/install.sh \
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

if [[ -t 1 || -r /dev/tty ]]; then
  C_RESET=$'\033[0m'
  C_BOLD=$'\033[1m'
  C_DIM=$'\033[2m'
  C_CYAN=$'\033[36m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_RED=$'\033[31m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_CYAN=""; C_GREEN=""; C_YELLOW=""; C_RED=""
fi

printf '\n%s========================================%s\n' "$C_CYAN" "$C_RESET"
printf ' %sSakuio Server 安装程序%s\n' "$C_BOLD" "$C_RESET"
printf '%s========================================%s\n\n' "$C_CYAN" "$C_RESET"

# Short command (`curl ... | sudo bash`) asks only for the essentials.
# When stdin is the script pipe, read prompts from /dev/tty.
ask_tty() {
  local prompt="$1" default="$2" answer=""
  if [[ -r /dev/tty && -w /dev/tty ]]; then
    printf '%b\n  %s>%s ' "$prompt" "$C_GREEN" "$C_RESET" > /dev/tty
    read -r answer < /dev/tty || true
  fi
  printf '%s' "${answer:-$default}"
}

SCRIPT_ONLY=0
if [[ "$INTERACTIVE" -eq 1 ]]; then
  DEFAULT_INSTALL_DIR="/opt/sakuio"
  INSTALL_DIR="$DEFAULT_INSTALL_DIR"

  # Re-running for an existing default installation goes straight to the
  # update menu. A custom directory is asked only when nothing is installed.
  if [[ ! -x "$INSTALL_DIR/sakuio-server" && ! -f "$INSTALL_DIR/config.yaml" ]]; then
    dir_input="$(ask_tty "${C_BOLD}请输入安装目录${C_RESET}${C_DIM}（直接回车使用 /opt/sakuio）${C_RESET}" "/opt/sakuio")"
    INSTALL_DIR="${dir_input:-/opt/sakuio}"
  fi

  if [[ -x "$INSTALL_DIR/sakuio-server" || -f "$INSTALL_DIR/config.yaml" ]]; then
    printf '\n%s已检测到安装%s：%s\n\n' "$C_YELLOW" "$C_RESET" "$INSTALL_DIR"
    printf '  %s1)%s 更新程序和管理脚本\n' "$C_GREEN" "$C_RESET"
    printf '  %s2)%s 只更新管理脚本\n' "$C_GREEN" "$C_RESET"
    printf '  %s0)%s 取消\n' "$C_RED" "$C_RESET"
    while true; do
      action="$(ask_tty "${C_BOLD}请选择更新方式${C_RESET}${C_DIM}（1=更新程序和脚本，2=只更新脚本，0=取消）${C_RESET}" "1")"
      case "$action" in
        1) SCRIPT_ONLY=0; break ;;
        2) SCRIPT_ONLY=1; break ;;
        0) echo "${C_DIM}已取消${C_RESET}"; exit 0 ;;
        *) echo "${C_RED}无效选项${C_RESET}" ;;
      esac
    done
  fi

  if [[ "$SCRIPT_ONLY" -eq 0 ]]; then
    if [[ -f "$INSTALL_DIR/config.yaml" ]]; then
      LISTEN="$(awk '/^listen:[[:space:]]*/{sub(/^listen:[[:space:]]*/, ""); print; exit}' "$INSTALL_DIR/config.yaml")"
      LISTEN="${LISTEN:-0.0.0.0:8555}"
    else
      port_input="$(ask_tty "${C_BOLD}请输入程序监听端口${C_RESET}${C_DIM}（直接回车使用默认端口 8555）${C_RESET}" "8555")"
      if [[ ! "$port_input" =~ ^[0-9]+$ ]] || (( port_input < 1 || port_input > 65535 )); then
        echo "${C_RED}错误：监听端口无效：$port_input${C_RESET}" >&2
        exit 1
      fi
      LISTEN="0.0.0.0:${port_input}"
    fi

    proxy_input="$(ask_tty "${C_BOLD}请输入 GitHub 加速站地址${C_RESET}${C_DIM}（不需要加速请直接回车）${C_RESET}" "")"
    [[ -n "$proxy_input" ]] && GITHUB_PROXY="$proxy_input"
  fi
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

if [[ "$SCRIPT_ONLY" -eq 0 ]]; then

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
  VERSION="${VERSION#v}"
  VERSION="${VERSION%/}"
  if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: failed to resolve latest version; pass --version" >&2
    exit 1
  fi
fi

ASSET="sakuio-server-${OS}-${ARCH}"
BASE="${GITHUB_BASE}/${REPO}/releases/download/v${VERSION}"
DOWNLOAD_URL="${BASE}/${ASSET}"

printf "${C_CYAN}==>${C_RESET} 正在安装 Sakuio Server %s (%s/%s)\n" "$VERSION" "$OS" "$ARCH"
printf "${C_DIM}    %s${C_RESET}\n" "$DOWNLOAD_URL"

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

if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_CYAN=$'\033[36m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_CYAN=""; C_GREEN=""; C_YELLOW=""; C_RED=""
fi

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
  # Ctrl-C stops the log viewer and returns to the menu, not the shell.
  trap ':' INT
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    tail -f "\$INSTALL_DIR/data/sakuio.log"
  else
    journalctl -u "\$SERVICE_NAME" -f --no-pager
  fi
  trap - INT
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
  local base latest_url effective api tag
  base="\$(github_base "\${GITHUB_PROXY:-}")"
  latest_url="\${base}/sakuio-monitor/Sakuio/releases/latest"
  effective=""
  if command -v curl >/dev/null 2>&1; then
    effective="\$(curl -fsSL -o /dev/null -w '%{url_effective}' "\$latest_url" 2>/dev/null || true)"
  else
    effective="\$(wget -qO- --max-redirs=5 "\$latest_url" 2>/dev/null || true)"
  fi
  if [[ "\$effective" =~ /tag/v([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    printf '%s' "\${BASH_REMATCH[1]}"
    return 0
  fi
  # Fall back to the GitHub API when the proxy does not expose the redirect.
  api="https://api.github.com/repos/sakuio-monitor/Sakuio/releases/latest"
  if command -v curl >/dev/null 2>&1; then
    tag="\$(curl -fsSL -H 'Accept: application/vnd.github+json' "\$api" 2>/dev/null | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"v\{0,1\}\([^"]*\)".*/\1/p' | head -1)"
  else
    tag="\$(wget -qO- "\$api" 2>/dev/null | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"v\{0,1\}\([^"]*\)".*/\1/p' | head -1)"
  fi
  if [[ "\$tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s' "\$tag"
    return 0
  fi
  return 1
}

do_update() {
  require_root
  local proxy="\${GITHUB_PROXY:-}"
  read -r -p "GitHub 加速地址（留空使用官方 GitHub）: " input_proxy
  [[ -n "\$input_proxy" ]] && proxy="\$input_proxy"
  local version
  read -r -p "目标版本（留空安装最新）: " version
  if [[ -z "\$version" ]]; then
    version="\$(GITHUB_PROXY="\$proxy" latest_version)" || true
  fi
  version="\${version#v}"
  version="\${version%/}"
  if [[ ! "\$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "无法获取有效版本号，请手动输入如 0.0.4" >&2
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

pause() {
  if [[ -t 0 ]]; then
    read -r -p "${C_DIM}按 Enter 返回菜单...${C_RESET}" _
  fi
}

menu() {
  local state
  if [[ "\$IS_DARWIN" -eq 1 ]]; then
    state="\$(launchctl print "system/\$SERVICE_NAME" >/dev/null 2>&1 && echo running || echo stopped)"
  else
    state="\$(systemctl is-active "\$SERVICE_NAME" 2>/dev/null || true)"
  fi
  local state_color
  if [[ "\${state:-}" == "active" || "\${state:-}" == "running" ]]; then
    state_color="\$C_GREEN"
  else
    state_color="\$C_RED"
  fi
  printf '\n\033[1;36m╭──────────────────────────────────────╮\033[0m\n'
  printf '\033[1;36m│\033[0m  \033[1mSakuio 管理控制台\033[0m\n'
  printf '\033[1;36m│\033[0m  服务状态：%b%s%b\n' "\$state_color" "\${state:-unknown}" "\$C_RESET"
  printf '\033[1;36m╰──────────────────────────────────────╯\033[0m\n'
  cat <<MENU
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
    1) svc_start; pause ;;
    2) svc_stop; pause ;;
    3) svc_restart; pause ;;
    4) svc_status; pause ;;
    5) svc_logs; pause ;;
    6) svc_logs100; pause ;;
    7) do_update; pause ;;
    8) do_username; pause ;;
    9) do_password; pause ;;
    10) do_disable_totp; pause ;;
    11) "\$BIN" admin list; pause ;;
    0) exit 0 ;;
    *) echo "无效选项"; pause ;;
  esac
done
CLIEOF
chmod 755 "$INSTALL_DIR/sakuio"
ln -sfn "$INSTALL_DIR/sakuio" /usr/local/bin/sakuio

printf '\n${C_GREEN}========================================${C_RESET}\n'
printf ' ${C_BOLD}Sakuio 安装完成${C_RESET}\n'
printf '========================================\n'
printf ' 监听地址: ${C_CYAN}%s${C_RESET}\n' "$LISTEN"
printf ' 管理命令: ${C_CYAN}sudo sakuio${C_RESET}\n'
printf ' 初始化:   ${C_CYAN}http://<服务器地址>:%s${C_RESET}\n\n' "$(echo "$LISTEN" | sed 's/.*://')"

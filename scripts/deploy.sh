#!/usr/bin/env bash
set -euo pipefail

# 用法:
# curl -sSL https://raw.githubusercontent.com/<YOUR_USER>/<YOUR_REPO>/main/scripts/deploy.sh | sudo REPO_URL="https://github.com/<YOUR_USER>/<YOUR_REPO>.git" bash
# 可选环境变量:
#   FORCE_APT=1  强制重新执行 apt-get update/install
#   FORCE_PIP=1  强制重新安装 requirements.txt 依赖

REPO_URL="${REPO_URL:-https://github.com/YOUR_USERNAME/poly-elder-quant.git}"
BRANCH="${BRANCH:-main}"
INSTALL_DIR="${INSTALL_DIR:-/opt/poly-elder-quant}"
SERVICE_NAME="poly-quant"
APP_PORT="${APP_PORT:-8080}"
FORCE_APT="${FORCE_APT:-0}"
FORCE_PIP="${FORCE_PIP:-0}"

T_START=$(date +%s)

echo "============================================================"
echo " Polymarket BTC 5m Quant Elder 自学习系统 - 一键部署/更新"
echo "============================================================"

echo "[1/6] 环境检查 (Swap + 基础依赖 + Chrony)..."
MEM_MB=$(LC_ALL=C free -m | awk '/^Mem:/ {print $2}')
SWAP_MB=$(LC_ALL=C free -m | awk '/^Swap:/ {print $2}')
if [ "${SWAP_MB:-0}" -lt 512 ] && [ ! -f /swapfile ] && [ "${MEM_MB:-0}" -lt 2500 ]; then
  echo "  -> 创建 2G Swap 防 OOM..."
  fallocate -l 2G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
  chmod 600 /swapfile
  mkswap /swapfile > /dev/null
  swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo "  -> 已启用 2G Swap"
fi

# 仅在缺少基础工具或强制刷新时才执行 apt-get（避免每次更新卡在 apt-get update 几十秒）
NEED_APT=0
if [ "$FORCE_APT" = "1" ]; then
  NEED_APT=1
else
  for bin in git python3 curl; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      NEED_APT=1
      break
    fi
  done
  if [ "$NEED_APT" = "0" ] && ! command -v chronyd >/dev/null 2>&1 && [ ! -x /usr/sbin/chronyd ]; then
    NEED_APT=1
  fi
  if [ "$NEED_APT" = "0" ] && ! python3 -c "import venv, ensurepip" >/dev/null 2>&1; then
    NEED_APT=1
  fi
fi

if [ "$NEED_APT" = "1" ]; then
  echo "  -> 首次安装缺失系统组件 (apt-get)..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get -o Acquire::ForceIPv4=true -o Acquire::http::Timeout=15 update -qq
  apt-get -o Acquire::ForceIPv4=true -o Acquire::http::Timeout=15 install -y -qq --no-install-recommends \
    git python3 python3-venv python3-pip chrony curl > /dev/null
else
  echo "  -> 基础依赖已就绪，跳过 apt-get"
fi

if command -v systemctl >/dev/null 2>&1 && ! systemctl is-active --quiet chrony 2>/dev/null; then
  systemctl enable --now chrony >/dev/null 2>&1 || true
fi

echo "[2/6] 拉取/更新 GitHub 代码 ($REPO_URL @ $BRANCH)..."
if [ -d "$INSTALL_DIR/.git" ]; then
  cd "$INSTALL_DIR"
  git remote set-url origin "$REPO_URL" 2>/dev/null || true
  git fetch --depth 1 origin "$BRANCH" -q
  git reset --hard FETCH_HEAD -q
else
  git clone --depth 1 -b "$BRANCH" "$REPO_URL" "$INSTALL_DIR"
  cd "$INSTALL_DIR"
fi

echo "[3/6] 检查 Python 3.11+ 虚拟环境与官方 SDK 依赖..."
VENV_CREATED=0

# polymarket-client 官方 SDK 需要 Python 3.11+；优先复用系统 3.11/3.12，不足时自动通过 uv 创建独立 Python 3.12 环境
NEED_NEW_VENV=0
if [ ! -x "$INSTALL_DIR/.venv/bin/python" ]; then
  NEED_NEW_VENV=1
else
  VENV_VER_OK=$("$INSTALL_DIR/.venv/bin/python" -c "import sys; print(1 if sys.version_info >= (3, 11) else 0)" 2>/dev/null || echo 0)
  if [ "$VENV_VER_OK" != "1" ]; then
    echo "  -> 检测到旧 .venv Python 版本低于 3.11，自动重建为 Python 3.12 环境以支持 polymarket-client SDK..."
    rm -rf "$INSTALL_DIR/.venv"
    NEED_NEW_VENV=1
  fi
fi

if [ "$NEED_NEW_VENV" = "1" ]; then
  PY_BIN=""
  for cand in python3.12 python3.11 python3; do
    if command -v "$cand" >/dev/null 2>&1; then
      if "$cand" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" 2>/dev/null; then
        PY_BIN="$cand"
        break
      fi
    fi
  done

  if [ -n "$PY_BIN" ]; then
    echo "  -> 使用系统 $PY_BIN 创建虚拟环境..."
    "$PY_BIN" -m venv "$INSTALL_DIR/.venv"
  else
    echo "  -> 系统 Python 低于 3.11，自动安装轻量 uv 并下载独立 Python 3.12 (不影响系统 Python)..."
    export PATH="$HOME/.local/bin:/root/.local/bin:$PATH"
    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null
      export PATH="$HOME/.local/bin:/root/.local/bin:$PATH"
    fi
    uv venv --python 3.12 --seed "$INSTALL_DIR/.venv"
  fi
  VENV_CREATED=1
fi

REQ_FILE="$INSTALL_DIR/requirements.txt"
REQ_STAMP="$INSTALL_DIR/.venv/.req_sha256"
CUR_REQ_HASH=$(sha256sum "$REQ_FILE" | awk '{print $1}')
OLD_REQ_HASH=$(cat "$REQ_STAMP" 2>/dev/null || true)

if [ "$VENV_CREATED" = "1" ] || [ "$FORCE_PIP" = "1" ] || [ "$CUR_REQ_HASH" != "$OLD_REQ_HASH" ]; then
  echo "  -> 检测到依赖变更，安装/更新 Python 包..."
  if [ "$VENV_CREATED" = "1" ]; then
    "$INSTALL_DIR/.venv/bin/pip" install --upgrade pip -q
  fi
  "$INSTALL_DIR/.venv/bin/pip" install -r "$REQ_FILE" -q
  echo "$CUR_REQ_HASH" > "$REQ_STAMP"
else
  echo "  -> requirements.txt 未变化，跳过 pip 安装"
fi

echo "[4/6] 初始化持久化数据目录与主加密密钥 (保留历史数据)..."
mkdir -p "$INSTALL_DIR/data/models"
if [ ! -f "$INSTALL_DIR/data/.env" ]; then
  MASTER_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
  cat <<EOF > "$INSTALL_DIR/data/.env"
APP_HOST=0.0.0.0
APP_PORT=${APP_PORT}
DEFAULT_PASSWORD=qq111111
MASTER_ENCRYPTION_KEY=${MASTER_KEY}
DATA_DIR=${INSTALL_DIR}/data
EOF
  chmod 600 "$INSTALL_DIR/data/.env"
fi

echo "[5/6] 检查 Systemd 守护进程配置..."
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TMP_UNIT=$(mktemp)
cat <<EOF > "$TMP_UNIT"
[Unit]
Description=Polymarket BTC 5m Quant Elder Self-Learning System
After=network-online.target chrony.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${INSTALL_DIR}/data/.env
Environment="OMP_NUM_THREADS=1"
Environment="OPENBLAS_NUM_THREADS=1"
Environment="MKL_NUM_THREADS=1"
Environment="NUMEXPR_NUM_THREADS=1"
Environment="OMP_WAIT_POLICY=PASSIVE"
ExecStart=${INSTALL_DIR}/.venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port ${APP_PORT} --workers 1
Restart=always
RestartSec=3
MemoryMax=850M
Nice=-2

[Install]
WantedBy=multi-user.target
EOF

UNIT_CHANGED=0
if [ ! -f "$UNIT_FILE" ] || ! cmp -s "$TMP_UNIT" "$UNIT_FILE"; then
  mv "$TMP_UNIT" "$UNIT_FILE"
  chmod 644 "$UNIT_FILE"
  UNIT_CHANGED=1
else
  rm -f "$TMP_UNIT"
fi

echo "[6/6] 启动/重启服务..."
if [ "$UNIT_CHANGED" = "1" ]; then
  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}" >/dev/null 2>&1 || true
fi
# Ensure zero stray/orphaned uvicorn processes outside systemd (prevents same-round double orders!)
systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
pkill -f "uvicorn backend.main:app" 2>/dev/null || true
sleep 1
systemctl restart "${SERVICE_NAME}"
sleep 1.5

ELAPSED=$(( $(date +%s) - T_START ))
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  PUBLIC_IP=$(curl -s --max-time 2 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')
  echo "============================================================"
  echo " ✅ 部署/更新成功！(耗时 ${ELAPSED}s)"
  echo " 🌐 Web 控制台: http://${PUBLIC_IP}:${APP_PORT}"
  echo " 🔑 默认密码:   qq111111 (登录后请立即在账号中心修改)"
  echo " 📜 查看日志:   journalctl -u ${SERVICE_NAME} -f"
  echo "============================================================"
else
  echo "❌ 服务启动失败，请执行 journalctl -u ${SERVICE_NAME} -n 50 查看原因"
  exit 1
fi

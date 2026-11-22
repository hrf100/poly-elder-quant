#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Polymarket BTC 5m Quant Elder — 服务器一键部署 / 更新
#
# 首次部署或日后更新，在 Linux VPS 执行同一行命令即可：
#
#   curl -sSL https://raw.githubusercontent.com/hrf100/poly-elder-quant/main/scripts/deploy.sh \
#     | sudo bash
#
# 可选环境变量：
#   REPO_URL=https://github.com/hrf100/poly-elder-quant.git
#   BRANCH=main
#   INSTALL_DIR=/opt/poly-elder-quant
#   APP_PORT=8080
#   GH_TOKEN=xxxx          # 仅私有仓库需要
# =============================================================================

REPO_URL="${REPO_URL:-https://github.com/hrf100/poly-elder-quant.git}"
BRANCH="${BRANCH:-main}"
INSTALL_DIR="${INSTALL_DIR:-/opt/poly-elder-quant}"
SERVICE_NAME="poly-quant"
APP_PORT="${APP_PORT:-8080}"
GH_TOKEN="${GH_TOKEN:-}"

if [[ "$REPO_URL" == *"YOUR_USERNAME"* ]]; then
  echo "❌ 请设置正确的 REPO_URL，例如: REPO_URL=https://github.com/hrf100/poly-elder-quant.git"
  exit 1
fi

if [[ "$(id -u)" -ne 0 ]]; then
  echo "❌ 请使用 root 或 sudo 执行本脚本"
  exit 1
fi

echo "============================================================"
echo " Polymarket BTC 5m Quant Elder — 一键部署/更新"
echo " Repo: $REPO_URL @ $BRANCH"
echo " Dir:  $INSTALL_DIR"
echo " Port: $APP_PORT"
echo "============================================================"

echo "[1/6] 低配服务器优化 (Swap + Chrony)..."
MEM_MB=$(free -m | awk '/Mem:/ {print $2}')
if [ ! -f /swapfile ] && [ "${MEM_MB:-0}" -lt 2500 ]; then
  fallocate -l 2G /swapfile 2>/dev/null || dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo "  -> 已创建 2G Swap"
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip chrony curl ca-certificates >/dev/null
systemctl enable --now chrony >/dev/null 2>&1 || true

clone_url="$REPO_URL"
if [ -n "$GH_TOKEN" ]; then
  # 支持私有仓库：https://github.com/owner/repo.git -> https://x-access-token:TOKEN@github.com/owner/repo.git
  clone_url=$(echo "$REPO_URL" | sed -E "s#https://github.com/#https://x-access-token:${GH_TOKEN}@github.com/#")
fi

echo "[2/6] 拉取/更新 GitHub 代码..."
if [ -d "$INSTALL_DIR/.git" ]; then
  cd "$INSTALL_DIR"
  # 更新时保留 data/ 与 .venv/，仅重置代码
  git remote set-url origin "$clone_url"
  git fetch --all --prune
  git checkout "$BRANCH"
  git reset --hard "origin/$BRANCH"
else
  mkdir -p "$(dirname "$INSTALL_DIR")"
  git clone --depth 1 -b "$BRANCH" "$clone_url" "$INSTALL_DIR"
  cd "$INSTALL_DIR"
fi
# 去掉凭证痕迹，避免 token 留在 remote url
git remote set-url origin "$REPO_URL"

echo "[3/6] 创建/更新 Python 虚拟环境并安装依赖..."
if [ ! -d "$INSTALL_DIR/.venv" ]; then
  python3 -m venv "$INSTALL_DIR/.venv"
fi
"$INSTALL_DIR/.venv/bin/pip" install --upgrade pip -q
"$INSTALL_DIR/.venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt" -q

echo "[4/6] 初始化持久化数据目录 (更新时自动保留 DB/密钥/模型)..."
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
  echo "  -> 已生成 data/.env (含 MASTER_ENCRYPTION_KEY)"
else
  # 确保端口与数据目录变量与本次部署一致
  grep -q '^APP_PORT=' "$INSTALL_DIR/data/.env" || echo "APP_PORT=${APP_PORT}" >> "$INSTALL_DIR/data/.env"
  sed -i "s/^APP_PORT=.*/APP_PORT=${APP_PORT}/" "$INSTALL_DIR/data/.env"
  grep -q '^DATA_DIR=' "$INSTALL_DIR/data/.env" || echo "DATA_DIR=${INSTALL_DIR}/data" >> "$INSTALL_DIR/data/.env"
  sed -i "s|^DATA_DIR=.*|DATA_DIR=${INSTALL_DIR}/data|" "$INSTALL_DIR/data/.env"
  echo "  -> 复用已有 data/.env / 数据库 / 模型"
fi

echo "[5/6] 写入 systemd 守护进程..."
cat <<EOF > "/etc/systemd/system/${SERVICE_NAME}.service"
[Unit]
Description=Polymarket BTC 5m Quant Elder Self-Learning System
After=network-online.target chrony.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${INSTALL_DIR}/data/.env
ExecStart=${INSTALL_DIR}/.venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port \${APP_PORT} --workers 1
Restart=always
RestartSec=3
MemoryMax=850M
Nice=-2

[Install]
WantedBy=multi-user.target
EOF

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
  ufw allow "${APP_PORT}/tcp" >/dev/null 2>&1 || true
  echo "  -> 已放行 ufw ${APP_PORT}/tcp"
fi

echo "[6/6] 重载并重启服务..."
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" >/dev/null
systemctl restart "${SERVICE_NAME}"
sleep 2

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  PUBLIC_IP=$(curl -s --max-time 4 https://api.ipify.org || hostname -I | awk '{print $1}')
  COMMIT=$(git -C "$INSTALL_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
  echo "============================================================"
  echo " ✅ 部署/更新成功！ commit=${COMMIT}"
  echo " 🌐 Web 控制台: http://${PUBLIC_IP}:${APP_PORT}"
  echo " 🔑 默认密码:   qq111111 (登录后请立即修改)"
  echo " 📜 查看日志:   journalctl -u ${SERVICE_NAME} -f"
  echo " ♻️  再次更新:   推送代码到 GitHub 后，重新执行本脚本即可"
  echo "============================================================"
else
  echo "❌ 服务启动失败，请执行: journalctl -u ${SERVICE_NAME} -n 80 --no-pager"
  exit 1
fi

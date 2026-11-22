# Polymarket BTC 5m Quant Elder 高频动量与整场自学习系统

专为 **Polymarket BTC 5 分钟涨跌二元期权** 与 **欧洲低配 VPS（1C1G / 2C2G）** 打造的轻量级量化监控、高仿真模拟与自迭代交易系统。

GitHub：https://github.com/hrf100/poly-elder-quant

## 核心特性

1. **基准价与结算价 100% 对齐 Polymarket 官方**：Gamma API + PolyBolt / RTDS 双预言机，严格官方结算标签。
2. **升级版无量纲 Quant Elder + 微结构领先入场 / 移动追踪止盈（v12）**。
3. **双层闭环自我学习**：5 秒整场轨迹录制 + 在线微循环 + 离线 Walk-Forward 训练。
4. **Web 全景控制台**：JWT 登录、钱包加密存储、模拟/实盘/暂停、风控与自学习面板。

规范文档：[docs/POLYMARKET_SPEC.md](docs/POLYMARKET_SPEC.md)

---

## 服务器一键部署 / 更新（推荐）

在 **Debian/Ubuntu 欧洲 VPS**（建议荷兰/芬兰；注意 Polymarket Geoblock，德国部分机房可能受限）执行：

### 首次部署

```bash
curl -sSL https://raw.githubusercontent.com/hrf100/poly-elder-quant/main/scripts/deploy.sh | sudo bash
```

默认安装到 `/opt/poly-elder-quant`，监听 **8080**，systemd 服务名 `poly-quant`。

自定义端口示例：

```bash
curl -sSL https://raw.githubusercontent.com/hrf100/poly-elder-quant/main/scripts/deploy.sh \
  | sudo APP_PORT=8000 bash
```

### 日后更新（同一行命令）

1. 本机改代码后推送到 GitHub `main`
2. 在服务器再执行一次上面的 `curl ... | sudo bash`

脚本会：

- `git fetch` + `reset --hard` 拉最新代码
- 更新 `.venv` 依赖
- **保留** `/opt/poly-elder-quant/data/`（数据库、加密密钥、模型、`.env`）
- `systemctl restart poly-quant`

### 常用运维命令

```bash
sudo systemctl status poly-quant
sudo journalctl -u poly-quant -f
sudo systemctl restart poly-quant
```

### 私有仓库部署（可选）

若仓库改为 private，需带 GitHub Token：

```bash
curl -sSL -H "Authorization: token $GH_TOKEN" \
  https://raw.githubusercontent.com/hrf100/poly-elder-quant/main/scripts/deploy.sh \
  | sudo GH_TOKEN="$GH_TOKEN" bash
```

---

## 本地快速启动

```bash
python -m venv .venv
# Windows: .\.venv\Scripts\pip install -r requirements.txt
# Linux:   source .venv/bin/activate && pip install -r requirements.txt

# Windows 示例
.\.venv\Scripts\uvicorn.exe backend.main:app --host 0.0.0.0 --port 8000 --reload
```

浏览器访问：`http://127.0.0.1:8000`（默认密码 `qq111111`）。

Linux 也可：

```bash
chmod +x scripts/run_local.sh
./scripts/run_local.sh   # 默认 8080
```

### 首次上手

1. 登录后立刻修改默认密码。
2. 模拟盘：点「一键生成零余额观察凭证」接通官方 TWAP。
3. 实盘：配置独立钱包私钥并派生 API Key，小额验证后再切 `LIVE`。

---

## 数据与密钥（切勿提交 Git）

| 路径 | 说明 |
|------|------|
| `data/poly_quant.db` | SQLite 主库（场次/切片/订单） |
| `data/.master_key` / `data/.env` | 加密主密钥与运行环境变量 |
| `data/models/` | 离线模型产物 |

`.gitignore` 已排除整个 `data/` 目录。服务器更新不会覆盖该目录。

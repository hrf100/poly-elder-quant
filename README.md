# Polymarket BTC 5m Quant Elder 高频动量与整场自学习系统

专为 **Polymarket BTC 5 分钟涨跌二元期权** 与 **欧洲低配 VPS（1C1G / 2C2G）** 打造的轻量级量化监控、高仿真模拟与自迭代交易系统。

## 核心特性

1. **基准价与结算价 100% 对齐 Polymarket 官方**：
   - 自动发现每 5 分钟新盘口（`btc-updown-5m-{ts}`），通过 **Gamma API** 锁定官方 `eventMetadata.priceToBeat`。
   - 直连新版 **Polymarket PolyBolt (`ws-live-v2`)** 订阅精确 `Decimal` 精度的 `btcusd` 实时价与 **60 秒 TWAP** 结算价。
   - 模拟盘、实盘结算与模型训练标签严格等待 Gamma API 官方最终裁定（`UP/DOWN`）。
2. **升级版无量纲 Quant Elder 动量引擎**：
   - 将经典 `EMA13 / Bull Power / Bear Power / Net Power` 升级为基点（`bps`）无量纲指标，结合 `ATR` 过滤与 **Kaufman 路径效率比（ER）** 区分单边趋势与画门震荡市。
   - 实时计算 `模型公平胜率 vs Polymarket 实时盘口赔率` 的期望值优势（Edge），结合 **Post-Only Maker 挂单** 规避 Taker 手续费与滑点。
3. **双层闭环自我学习 & 整场 300s 波动学习**：
   - **每 5 秒整场轨迹录制**：无论是否开单，持续采集完整 300 秒路径特征（`MFE/MAE`、`ER`、`TWAP 锁定进度`、L2 盘口深度不平衡）。
   - **在线微循环（每 5 分钟）**：根据官方结算结果自动更新在线概率校准权重、**汤普森采样（Thompson Sampling）报价偏移与撤单超时（TTL）策略臂**、分时段自适应阈值及**动态分数凯利仓位**。
   - **离线宏循环（每 12 小时）**：低优先级后台进程执行严格按场次时间切分（Purged Walk-Forward，零数据泄露）的 **LightGBM** 整场轨迹训练，支持 Champion/Challenger 自动迭代与一键回滚。
4. **Web 全景控制台与安全加固**：
   - 默认密码 `qq111111`（防暴力破解锁定 + JWT HttpOnly Cookie + 顶部改密提醒）。
   - 钱包私钥与 API 凭证经 `AES-256-GCM` 加密落盘；支持**一键生成零余额观察凭证**，纯模拟盘也能立刻直连 PolyBolt 官方 60s TWAP 流。
   - 实时监控 CPU / 内存 / Swap / 实时与累计网络流量、数据源延迟、收益曲线、持久化开单记录与 Polymarket 余额/持仓组合。

---

## 快速使用指南

### 方式一：上传到你的 GitHub 后，在欧洲 VPS 一行命令部署 & 更新

1. 将本项目代码推送到你自己的 GitHub 仓库（公开或配置 Deploy Key 的私有库）。
2. 把下方命令中的 `<你的用户名>/<仓库名>` 替换后，在欧洲 VPS（建议德国/荷兰/芬兰，避开 Polymarket 封锁区）执行：

```bash
curl -sSL https://raw.githubusercontent.com/<你的用户名>/<仓库名>/main/scripts/deploy.sh | sudo REPO_URL="https://github.com/<你的用户名>/<仓库名>.git" bash
```

> **日后更新代码**：推送新代码到 GitHub 后，在服务器再次运行**同一行命令**即可无感拉取更新并重启，历史数据库、加密私钥与模型版本全部自动保留在 `/opt/poly-elder-quant/data/`。

### 方式二：本地快速启动测试

```bash
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

启动后浏览器访问：`http://127.0.0.1:8080`（默认登录密码：`qq111111`）。

### 首次上手 3 步操作

1. **修改默认密码**：登录后进入「账号与安全」标签页修改默认密码 `qq111111`。
2. **激活官方 PolyBolt 60s TWAP 流**：
   - **纯模拟阶段**：直接点击「一键生成零余额观察凭证」，0 风险直连官方 TWAP 预言机。
   - **实盘阶段**：填入独立隔离钱包的私钥、选择钱包类型（网页端充值通常为 `2: Gnosis Safe` 并填写 Funder 地址），点击「保存并派生 API Key」，刷新验证 Polygon `USDC.e` 余额与授权状态。
3. **72 小时模拟积累样本**：保持默认 `PAPER` 模式运行 2~3 天，待录制 200+ 场完整轨迹后点击「立即执行离线 Walk-Forward 训练」观察 Brier Score 与模拟收益曲线，确认稳定后再以小额切换至 `LIVE`。

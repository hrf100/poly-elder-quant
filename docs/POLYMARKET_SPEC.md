# Polymarket 规则、玩法、微观结构与技术接口标准文档

> **文档用途**：作为本量化项目（`poly-elder-quant`）策略研究、回测建模与实盘工程开发的权威基准依据。  
> **核查基准日**：2026-11-21（已对齐 Polymarket 官方 CLOB V2、pUSD 抵押体系、PolyBolt Live Data、BTC 5m 60s TWAP 规则及 2026-11-20 生效的最新费率与返佣政策）。  
> **官方文档入口**：`https://docs.polymarket.com`（完整索引：`https://docs.polymarket.com/llms.txt`）。

---

## 一、2026 生产现状速览（开发与策略必读）

| 维度 | 2026 现网标准 | 对策略/工程的关键影响 |
| --- | --- | --- |
| **撮合引擎与 SDK** | **CLOB V2**（2026-04-28 全量上线生产，`https://clob.polymarket.com`） | 旧版 V1 SDK（`py-clob-client`）与 V1 签名订单**已不可用于生产**；必须使用 `py-clob-client-v2` / V2 EIP-712 签名。 |
| **抵押资产** | **pUSD**（`0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`，1:1 USDC 支撑） | 替代历史 `USDC.e` 作为交易抵押、Split/Merge/Redeem 与返佣结算币种；API 交易者通过 Collateral Onramp `wrap()` 封装。 |
| **BTC 5m 结算规则** | **Chainlink BTC/USD 60s TWAP**（2026-08-14 00:00 UTC 起） | `final_twap60 >= price_to_beat` 判 **UP**（平局判 UP），`<` 判 **DOWN**；上场终价严格等于下场基准价。 |
| **交易手续费** | `fee = C × feeRate × p × (1 - p)`（2026-11-20 23:59 UTC 最新表） | **Maker 永远 0 费**；BTC 5m/15m Taker 系数 `0.05`（在 `p=0.50` 达峰值 `$0.0125/股`，即名义额 `2.5%`）。 |
| **吃单保护延迟** | 加密/金融 Up-Down 市场启用 **250ms Taker Delay**（`itode: true`） | 可成交订单在撮合引擎暂存 250ms 后复检撮合，期间不可撤单；延迟套利必须将 `250ms + 网络 RTT` 纳入滑点模型。 |
| **实时预言机流** | **PolyBolt**（`wss://ws-live-v2.polymarket.com/ws`）取代旧 RTDS | 需 CLOB API 凭证认证，**订阅必须显式指定 `provider`**（`chainlink` / `binance` / `pyth`）；Chainlink 流需额外 Chainlink API Key。 |
| **机房与合规** | 主撮合位于 **AWS `eu-west-2`（伦敦）** | 伦敦/德国/法国/新加坡/美英均属 API **Close-Only** 封锁区；最近无 API 限制区域为 **AWS `eu-west-1`（爱尔兰）**，或荷兰 `NL` / 日本 `JP`（仅前端受限，API 放行）。 |

---

## 二、市场结构、玩法与结算机制

### 2.1 三类核心市场结构

1. **标准二元市场（Binary Market）**
   - 仅含两个互斥结果：`YES / NO`（加密短周期市场命名为 `Up / Down`）。
   - 每个结果对应独立的 ERC-1155 Token，完整一对 `1 YES + 1 NO` 恒等于 `1 pUSD` 抵押品。
   - 极端情况下二元市场可按比例拆分赔付（Split Payout）；正常情况胜出方赎回 `$1.00`，败方归 `$0.00`。
2. **多结果负风险市场（Negative Risk / NegRisk）**
   - 适用于多选一互斥事件（如选举、锦标赛、多档价格区间）。
   - 核心资本效率机制：**持有任意 1 份结果 A 的 `NO` 份额，可原子转换为除 A 以外所有其他结果的各 1 份 `YES` 份额**（通过 `NegRiskModule` / `NegRiskAdapter`）。
   - 支持 **Augmented Negative Risk**（允许事件开始后动态新增候选项 + `Other` 兜底项），且永不拆分赔付（有且仅有一个胜者拿走 `$1.00`）。
3. **组合市场（Combos）**
   - 多腿组合头寸；无论底层腿品类如何，统一使用 Taker 费率系数 `0.05`，且**不计入 Maker Rebate**。

### 2.2 资产与持仓生命周期（ERC-1155）

Polymarket 同时存在两套 ERC-1155 账本（不可混用 ID，以市场元数据为准）：
- **CTF 体系**：`Conditional Tokens` 合约（`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`），标识为 `CTF token ID`。
- **Polymarket V2 体系**：`PositionManager` 合约（`0x006F54F7f9A22e0000CC2AB60031000000ae9fEF`），标识为 `Position ID`，通过 `Router` 统一入口操作。

三种链上头寸转换操作（均 1:1 无平台费，仅需 Polygon Gas）：
- **Split（铸造）**：`1 pUSD → 1 YES + 1 NO`（双边做市商获取初始库存的标准方式）。
- **Merge（合并）**：`1 YES + 1 NO → 1 pUSD`（双边锁利套利后无需等待结算，**立即释放资金循环复用**）。
- **Redeem（赎回）**：市场结算后，胜出 Token 按 `1 Token = 1 pUSD` 赎回；**无赎回过期时间**，V2 还提供 `AutoRedeemer` 合约支持自动赎回。

### 2.3 双轨结算预言机与 BTC 5 分钟市场精解

Polymarket 采用双轨裁定：
- **通用预测市场**：采用 **UMA Optimistic Oracle**（提议 → 挑战期 → 争议投票，存在数小时结算延迟与争议风险）。
- **加密/金融 Up-Down 市场**：采用 **Chainlink TWAP 确定性结算**，无需 UMA 争议流程，规则完全数学化。

#### BTC 5 分钟涨跌市场（`btc-updown-5m-{start}`）官方规则

| 项目 | 规则定义（2026-08-14 00:00 UTC 起生效） |
| --- | --- |
| **场次周期** | 每场严格 `300s`，起点为能被 `300` 整除的 Unix 时间戳（`00:00:00, 00:05:00, ...`），每日 `288` 场。 |
| **Market Slug** | `btc-updown-5m-{window_start_unix_sec}`（例：`btc-updown-5m-1789878900`）。注意前端标题为美东时间（ET），程序必须统一按 **UTC Unix 秒** 处理。 |
| **唯一裁定源** | Chainlink BTC/USD 60 秒时间加权均价流（`btc-usd-twap-60s-streams`）。交易所现货价、Chainlink 普通 spot 均**不作为结算依据**。 |
| **基准价（Price to Beat）** | 场次开始整点 `T = 0` 的 `TWAP60`（实质反映 `[T-60s, T]` 过去 60 秒均价）。 |
| **结算价（Final Price）** | 场次结束整点 `T = 300` 的 `TWAP60`（实质反映 `[T+240s, T+300s]` 最后 60 秒均价）。 |
| **胜负与平局判定** | `Final Price >= Price to Beat` → **UP 胜**（平局归 UP）；`Final Price < Price to Beat` → **DOWN 胜**。 |
| **跨场精确衔接** | 第 `N` 场的官方 `finalPrice` 逐位精确等于第 `N+1` 场的 `priceToBeat`。 |

> **历史规则变更（回测跨期必看）**：
> - `2026-02-12 ~ 2026-08-06`：单点快照（Chainlink `btc-usd` spot）；
> - `2026-08-07 ~ 2026-08-13`：30s TWAP（`btc-usd-twap-30s-streams`，仅 5m 市场过渡一周；15m/4h 直接切 60s）；
> - `2026-08-14 00:00 UTC` 至今：60s TWAP（`btc-usd-twap-60s-streams`）。

### 2.4 撮合引擎、订单类型与 250ms Taker Delay

1. **撮合与价格改善（Price Improvement）**
   - 订单簿采用价格优先、时间优先的中央限价订单簿（CLOB），链下高速撮合、链上批量结算。
   - **价格改善归 Taker 所有**：若买单出价 `$0.55` 吃进挂在 `$0.52` 的卖单，实际按 `$0.52` 成交。
   - **互补合成撮合**：`BUY UP @ p` 在底层等价于 `SELL DOWN @ (1 - p)`，撮合引擎可在互补盘口间自动合成撮合（铸造/合并撮合）。
2. **四种订单类型（`OrderType`）**
   - **`GTC`（Good-Til-Cancelled）**：限价挂单，持续有效直至成交或主动撤单。
   - **`GTD`（Good-Til-Date）**：带自动过期时间戳（秒级 `expiration`）的限价单。注意 Polymarket 要求 `expiration` 至少大于当前时间 + 安全缓冲（常规需 `>= now + 60s` 以上）。
   - **`FOK`（Fill-Or-Kill）**：市价/可成交限价单，必须**立即全额成交**，否则整单拒绝取消（不留残单）。
   - **`FAK`（Fill-And-Kill / IOC）**：立即尽可能成交现有深度，未成交部分立刻自动撤销。
   - **`postOnly: true`**（仅限 `GTC` / `GTD`）：强制只做 Maker；若提交瞬间会与对手盘撮合，**直接拒单**，彻底杜绝意外吃单付费。
3. **250ms Taker Delay（`itode: true`）**
   - 选定的加密和金融 Up/Down 市场启用 **250ms 吃单暂存延迟**（可通过 `GET /clob-markets/{condition_id}` 的 `itode` 字段查询）。
   - 可成交订单到达后被引擎 Hold **250ms**，期间**不可撤单**；250ms 期满后重新校验盘口、余额、授权与风控再执行撮合或挂单，HTTP 接口会同步等待最终结果返回。

---

## 三、费用、返佣与激励体系（2026-11-20 23:59 UTC 现行版）

### 3.1 动态 Taker 手续费公式

$$\text{fee}_{\text{USDC}} = C \times \text{feeRate} \times p \times (1 - p)$$

- \(C\)：成交股数（shares），\(p \in (0, 1)\)：成交价（美元），\(\text{feeRate}\)：品类系数（由协议在撮合时动态计算，**不再签入订单结构体**）。
- **Maker 费用永远为 `0`**；仅 Taker 付费。
- 精度四舍五入至 **5 位小数**，最小收费 `0.00001`（低于该值取 0）。
- **买入时等效每股净成本**：\(p_{\text{eff}} = p + \text{feeRate} \cdot p(1 - p)\)，即**等效名义费率** \(= \text{feeRate} \cdot (1 - p)\)。

#### 全品类 Taker 费率系数表

| 市场品类 | Taker `feeRate` | Maker `feeRate` | 备注 |
| --- | :---: | :---: | --- |
| **Crypto — BTC 5m / 15m Up/Down** | **`0.05`** | `0` | 本项目核心战场 |
| Crypto — 其他所有市场 | `0.01` | `0` | — |
| Sports / Finance / Tech / Politics(选举) / Combos / Other | `0.05` | `0` | Combos 无 Maker 返佣 |
| Politics(非选举/地缘) / Economics / Culture / Weather / Mentions | `0.01` | `0` | — |
| Geopolitics（地缘政治） | `0` | `0` | 免费且无返佣 |

#### BTC 5m（`feeRate = 0.05`）关键价位手续费与盈亏平衡门槛

| 买入价 \(p\) | 买 100 股花费 | Taker 手续费（USDC） | 名义费率 \(\frac{\text{fee}}{C\cdot p}\) | 税后盈亏平衡胜率 \(p_{\text{eff}}\) | 相比盘口需额外跨越的 Edge |
| :---: | :---: | :---: | :---: | :---: | :---: |
| `$0.20` | `$20` | `$0.80` | `4.00%` | `20.80%` | `+0.80%` |
| `$0.35` | `$35` | `$1.14` | `3.25%` | `36.14%` | `+1.14%` |
| **`$0.45`** | `$45` | `$1.24` | `2.75%` | `46.24%` | **`+1.24%`** |
| **`$0.50`** | `$50` | **`$1.25`**（峰值） | `2.50%` | `51.25%` | **`+1.25%`** |
| **`$0.60`** | `$60` | `$1.20` | `2.00%` | `61.20%` | **`+1.20%`** |
| **`$0.70`** | `$70` | `$1.05` | `1.50%` | `71.05%` | **`+1.05%`** |
| `$0.85` | `$85` | `$0.64` | `0.75%` | `85.64%` | `+0.64%` |

> **关键策略洞见**：单看费率，在 `[0.45, 0.72]` 区间内，吃单（Taker）仅需多支付 **`1.0% ~ 1.25%`** 的绝对胜率门槛（外加 1~2c 买卖价差与延迟滑点），远小于被动 Maker 遭遇的逆向选择损失（回测实测逆向选择可吃掉 `10%+` 胜率）。

### 3.2 阶梯返佣体系（Maker & Taker Rebates）

30 天加权成交量公式（按成交股数与价格方差加权，**取消品类乘数**，Maker 与 Taker 分别独立累计）：

$$wV = \sum C \times p \times (1 - p)$$

- **Maker Rebates**（每日以 `pUSD` 发放，最低 `$1` 起发；按你的挂单为平台带来的 Taker 费总额分成）：
  - `< $200k`: `0%` ｜ `Tier 1 ($200k–$1M)`: `3%` ｜ `Tier 2 ($1M–$4M)`: `7%` ｜ `Tier 3 ($4M–$10M)`: `10%` ｜ `Tier 4 ($10M+)`: `15%`
- **Taker Rebates**（按已付 Taker 费比例返还）：
  - `< $200k`: `0%` ｜ `Gold ($200k–$1M)`: `10%` ｜ `Platinum ($1M–$4M)`: `25%` ｜ `Diamond ($4M–$10M)`: `40%` ｜ `Obsidian ($10M–$75M)`: `50%` ｜ `Onyx ($75M+)`: `60%`

### 3.3 流动性奖励（Liquidity Rewards）

- 面向在激励市场中贴近中间价（Midpoint）挂出静置限价单的做市商，每日 UTC 00:00 结算发放（最低 `$1`）。
- 需满足单市场配置的 `rewards_min_size`（最小单量）和 `max_spread`（最大偏离中价距离），**双边对称挂单（同时挂买和卖）有显著评分加成**。
- 加密与金融市场的 250ms Taker Delay **不额外增加**流动性奖励的最短挂单年龄门槛，挂上即开始计分。

---

## 四、量化策略框架与 BTC 5m 微观结构依据

### 4.1 主流 Polymarket 量化策略矩阵

| 策略类型 | 收益来源（Alpha） | 执行角色 | 核心风险 | 适合资金规模 |
| --- | --- | --- | --- | --- |
| **1. 现货领先动量 / 延迟套利**（本项目主策略） | Binance/OKX 现货 1s 动量领先 Polymarket CLOB 盘口定价 | **主动 Taker（FOK/FAK）**为主；宽价差用短 TTL 激进挂单 | 250ms Taker Delay 滑点、假突破画门、模型过拟合 | 中小资金（`$500 – $50k`） |
| **2. 盘口微结构做市 + 返佣/奖励** | 赚取买卖价差（Spread）+ 0 费率 + Liquidity Rewards / Maker Rebate | **Post-Only Maker** + 动态库存偏斜（Inventory Skew） | 强单边趋势下的逆向选择（被延迟套利者收割） | 中大资金（`$10k+`） |
| **3. 双边无风险锁利（Pair Arbitrage）** | 同场次内分时买齐等份 `UP + DOWN`，满足 `p_up + p_down < 1` 后 Merge 或等结算 | Maker 挂反手或极端波动时双边扫单 | 瘸腿风险（单边成交后反手再无机会） | 任意资金 |
| **4. 尾盘高确定性折价套利（Late-Stage TWAP Lock）** | 最后 30~60s 中 `TWAP60` 大部分已锁定，理论胜率 `99%+` 但盘口因流动性不足折价到 `0.95~0.97` | 精准算 `required_flip_bps` 后 Taker 吃折价 | 黑天鹅剧烈插针（单次亏损 20~30 倍利润） | 中大资金 |
| **5. NegRisk 多结果合成套利** | 多选项市场 `∑YES_ask < 1` 或 `NO → 其他 YES` 转换套利 | 多腿原子或并发执行 | 部分腿成交失败、链上转换 Gas 与时延 | 中大资金 |

### 4.2 BTC 5m 市场的关键数学与微观结构约束

1. **60s TWAP 方差桥（Variance Bridge）**
   - 当剩余时间 \(T_{\text{rem}} > 60\text{s}\) 时，最后 60 秒均价的方差近似等价于缩短了约 `40s` 的布朗运动剩余时间：\(\sigma_{\text{rem}} \approx \sigma_{1s}\sqrt{T_{\text{rem}} - 40}\)。
   - 当 \(T_{\text{rem}} \le 60\text{s}\) 时，已有 \(w_{\text{locked}} = \frac{60 - T_{\text{rem}}}{60}\) 的结算权重被历史价格永久锁定，剩余不确定性按 \(\sigma_{\text{rem}} \approx \frac{T_{\text{rem}}}{60}\cdot \sigma_{1s}\sqrt{\frac{T_{\text{rem}}}{3}}\) 急速坍缩（路径方差为端点的 \(1/3\)）。
2. **市场效率与模型锚定原则**
   - Polymarket 盘口 `mid` 聚合了大量专业做市商（含直连 `eu-west-2` 托管节点）的信息，是极强先验。
   - **严禁独立从零预测绝对胜率去和市场对赌**（小样本树模型极易过拟合且制造 `15%~25%` 假 Edge）；正确范式为 **Market-Anchored Residual Learning**：\(\text{logit}(p_{\text{fair}}) = \text{logit}(p_{\text{mid}}) + \Delta(\text{spot\_momentum}, \text{microstructure})\)，并对相对市场的偏离设置硬上限（如 `±9%`）。
3. **执行角色必须匹配信号属性**
   - **信息领先型信号（现货动量）必须优先 Taker**（或极短 TTL 激进跟价），绝对避免在买一挂长 TTL 被动单，否则必然落入“真突破踏空、假突破接刀”的逆向选择陷阱。
   - 仅在无方向偏好、赚价差/返佣时使用 `postOnly: true` 的双边做市。

---

## 五、官方技术架构与接口标准

### 5.1 四大官方服务域总览

| 服务 | Base URL / Endpoint | 认证要求 | 核心职责 |
| --- | --- | --- | --- |
| **CLOB REST (V2)** | `https://clob.polymarket.com` | 读公开 / 写 L2 + 订单 EIP-712 | 订单簿、报价、下单/撤单、成交查询、市场约束、心跳、限流查询 |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market`<br>`wss://ws-subscriptions-clob.polymarket.com/ws/user` | Market 公开 / User 需 L2 凭证 | 毫秒级 L2 订单簿推送、成交价、Tick 变更、个人挂单/成交状态回报 |
| **Gamma API** | `https://gamma-api.polymarket.com` | 公开无需认证 | 市场/事件发现、Slug 查询、`clobTokenIds`、`eventMetadata.priceToBeat/finalPrice`、结算状态 |
| **Data API v2** | `https://data-api.polymarket.com/v2` | 公开无需认证 | 钱包持仓（`OPEN/REDEEMABLE/MERGEABLE/CLOSED`）、组合估值、历史成交、PnL、持仓大户、价格历史 |
| **PolyBolt Live Data** | `wss://ws-live-v2.polymarket.com/ws` | 需 CLOB API 凭证（Chainlink 源另需 Chainlink Key） | 加密/股票实时参考价与 **60s Chainlink TWAP** 精确 Decimal 流 |
| **RTDS（Legacy）** | `wss://ws-live-data.polymarket.com` | 公开无需认证 | 旧版实时参考价流（官方已建议迁移至 PolyBolt，暂可作免 Key 容灾备用） |
| **Geoblock Check** | `https://polymarket.com/api/geoblock` | 公开 | 检测出口 IP 所属国家与是否被封锁（注意域名在 `polymarket.com`） |

### 5.2 认证体系与钱包类型（`signature_type`）

#### 四种 `signature_type`

| `signature_type` | 枚举名 | 适用场景 | `funder`（Maker 地址） |
| :---: | --- | --- | --- |
| `0` | `EOA` | 普通外部钱包（MetaMask / 本地私钥直连，自己付 Gas） | 与 Signer 相同 |
| `1` | `POLY_PROXY` | Magic / Email 登录生成的 Polymarket Proxy 合约钱包 | 网页端 Proxy 地址 |
| `2` | `POLY_GNOSIS_SAFE` | 浏览器钱包连接 Polymarket 网页端自动部署的 Gnosis Safe（最常见） | 网页端 Safe 地址 |
| `3` | `POLY_1271` | ERC-1271 智能合约存款钱包（Deposit Wallet） | 合约钱包地址 |

#### L1 / L2 两层认证协议

1. **L1 认证（创建/派生 API Key）**：用私钥对 EIP-712 `ClobAuth`（Domain: `name="ClobAuthDomain", version="1", chainId=137`）签名，携带头：
   - `POLY_ADDRESS` / `POLY_SIGNATURE` / `POLY_TIMESTAMP` / `POLY_NONCE`
   - 调用 `POST /auth/api-key` 或 `GET /auth/derive-api-key` 获取 `{apiKey, secret, passphrase}`。
2. **L2 认证（所有私有 REST 请求）**：
   - 待签明文：`message = str(unix_sec) + METHOD_UPPER + request_path + exact_raw_body`
   - 签名算法：`urlsafeBase64WithPadding(HMAC_SHA256(base64Decode(secret), message))`
   - 请求头：`POLY_ADDRESS`、`POLY_SIGNATURE`、`POLY_TIMESTAMP`、`POLY_API_KEY`、`POLY_PASSPHRASE`。

#### CLOB V2 订单 EIP-712 签名标准（2026-04-28 起强制）

- **Domain**：
  - `name`: `"Polymarket CTF Exchange"`，**`version`: `"2"`**（V1 的 `"1"` 已废弃），`chainId`: `137`
  - `verifyingContract`：标准市场 `0xE111180000d2663C0091e4f400237545B87B996B`；NegRisk 市场 `0xe2222d279d744050d28e00520010520000310F59`。
- **V2 `Order` 结构体**（移除了 `nonce`、`feeRateBps`、`taker`、`expiration`；新增 `timestamp`、`metadata`、`builder`）：
  ```solidity
  Order(
    uint256 salt,
    address maker,
    address signer,
    uint256 tokenId,
    uint256 makerAmount,
    uint256 takerAmount,
    uint8   side,           // 0 = BUY, 1 = SELL（HTTP JSON body 中写 "BUY"/"SELL"）
    uint8   signatureType,
    uint256 timestamp,      // 毫秒时间戳，替代 nonce 保证唯一性
    bytes32 metadata,       // 默认 0x00...00
    bytes32 builder         // 可选 Builder Code，默认 0x00...00
  )
  ```
- **HTTP `POST /order` Body**：仍可在 JSON 的 `order` 中传 `"expiration": "0"`（仅用于 GTD 过期控制），顶层传 `"owner": "<apiKey>"`、`"orderType": "GTC"|"GTD"|"FOK"|"FAK"`、`"postOnly": true|false`。
- **官方 Python SDK**：使用 **`pip install py-clob-client-v2`**（Node 用 `@polymarket/clob-client-v2`），不再使用旧版 `py-clob-client`。

### 5.3 核心 REST 接口清单

#### 1) Gamma API（市场发现与元数据）
- `GET /events?slug=btc-updown-5m-{ts}`（查已结束场次可加 `closed=true` 或 `closed=any`）
  - 关键返回字段：`eventMetadata.priceToBeat`、`eventMetadata.finalPrice`、`markets[0].conditionId`、`markets[0].clobTokenIds`、`markets[0].outcomes`、`markets[0].outcomePrices`、`markets[0].orderPriceMinTickSize`、`markets[0].orderMinSize`、`markets[0].negRisk`、`markets[0].acceptingOrders`。
  - **重要工程规范**：`clobTokenIds` 必须按 `outcomes` 数组中的标签（`"Up"` / `"Down"`）做显式对齐映射，不可硬编码下标 `0/1`。
- `GET /markets?closed=false&tag_id=...&liquidity_num_min=...`：批量筛选活跃市场。

#### 2) CLOB REST API（行情与交易）
- **行情与市场约束（公开）**：
  - `GET /book?token_id={tid}` / `POST /books`：L2 订单簿 + `tick_size` + `min_order_size` + `neg_risk`
  - `GET /clob-markets/{condition_id}` / `GET /markets/{condition_id}`：市场详情、`itode`（是否启用 250ms Taker Delay）、`tokens[].winner`（官方结算胜方校验）
  - `GET /tick-size?token_id={tid}`、`GET /neg-risk?token_id={tid}`、`GET /fee-rate?token_id={tid}`
  - `GET /midpoint`、`GET /price`、`GET /spread`、`GET /last-trade-price`
- **交易执行（L2 Auth）**：
  - `POST /order`（单笔下单）、`POST /orders`（批量下单，受单批 burst 限制）
  - `DELETE /order`、`DELETE /orders`、`DELETE /cancel-all`、`DELETE /cancel-market-orders`
  - `GET /data/order/{order_id}`（查询单笔订单真实成交量与状态）、`GET /data/orders`、`GET /data/trades`
  - `GET /balance-allowance` / `GET /balance-allowance/update`
  - **`POST /v1/heartbeats`**（心跳保活，详见 5.6 节）
  - `GET /v1/user/rate-limit-tier`（查询当前 Signer 限流等级）

#### 3) Data API v2（账务与分析，无需认证）
- `GET /v2/positions?user={addr}&status=OPEN`（支持 `OPEN` / `REDEEMABLE` / `REDEEMABLE_LOST` / `MERGEABLE` / `CLOSED`，返回 `{data, pagination}` 游标分页）
- `GET /v2/value?user={addr}`（组合总估值）、`GET /v2/approvals?user={addr}`（授权状态）
- `GET /v2/trades`、`GET /v2/activity`、`GET /v2/user-pnl`、`GET /v2/prices-history`、`GET /v2/resolutions`、`GET /v2/oi`

### 5.4 订单价格精度、最小单量与 6 位整数编码规范

订单提交前**必须**对齐市场当前 `tick_size`（且监听 WS `tick_size_change` 事件实时更新），否则 CLOB 直接返回 `400` 拒单：

| `tick_size` | 价格小数位（Price） | 股数小数位（Size） | 金额小数位（Amount） |
| :---: | :---: | :---: | :---: |
| `0.1` | 1 | 2 | 3 |
| **`0.01`**（5m 常规） | **2** | **2** | **4** |
| `0.005` | 3 | 2 | 5 |
| `0.0025`（特定体育） | 4 | 2 | 6 |
| `0.001` | 3 | 2 | 5 |
| `0.0001` | 4 | 2 | 6 |

- `makerAmount` / `takerAmount` 均乘以 \(10^6\) 编码为整数字符串：
  - **BUY**：`makerAmount = price × size`（pUSD），`takerAmount = size`（shares）
  - **SELL**：`makerAmount = size`（shares），`takerAmount = price × size`（pUSD）
- 校验：`size >= min_order_size`（常规市场通常为 `5` shares；同时满足最小名义额约束）。

### 5.5 WebSocket 协议标准

#### 1) CLOB Market Channel（`wss://ws-subscriptions-clob.polymarket.com/ws/market`）
- 订阅帧：
  ```json
  {
    "assets_ids": ["<up_token_id>", "<down_token_id>"],
    "type": "market",
    "initial_dump": true,
    "custom_feature_enabled": true
  }
  ```
- 心跳：每 10~20s 发送文本帧 `"PING"`，服务端回 `"PONG"`。
- 核心事件（`event_type`）：
  - `book`：全量/初始盘口深度快照（`bids`, `asks`）
  - `price_change`：增量档位变化（含 `best_bid`, `best_ask`）
  - `last_trade_price`：最新成交价（**注意记录接收时间戳，防止撮合复用陈旧成交价**）
  - `tick_size_change`：价格最小变动单位切换（如极端价位 `0.01 → 0.001`）
  - 开启 `custom_feature_enabled: true` 后额外推送：`best_bid_ask`、`new_market`、`market_resolved`。

#### 2) CLOB User Channel（`wss://ws-subscriptions-clob.polymarket.com/ws/user`）
- 订阅帧：
  ```json
  {
    "auth": { "apiKey": "...", "secret": "...", "passphrase": "..." },
    "type": "user",
    "markets": ["<optional_condition_id>"]
  }
  ```
- **实盘必备**：实时推送账户挂单确认、部分成交、全部成交（含真实成交均价 `price` 与成交量 `size_matched`）与撤单事件；**严禁在实盘把 `POST /order` 返回成功直接当作 `FILLED`**。

#### 3) PolyBolt Live Data（`wss://ws-live-v2.polymarket.com/ws`）与 RTDS 对照

| 维度 | RTDS（Legacy：`wss://ws-live-data.polymarket.com`） | PolyBolt（新版：`wss://ws-live-v2.polymarket.com/ws`） |
| --- | --- | --- |
| **认证** | 公开免 Key | 需先发 `{"op":"auth","rid":"a1","auth":{...}}`（CLOB 凭证）；Chainlink 源需 Chainlink API Key |
| **心跳** | 客户端每 5s 发文本 `"PING"` | 服务端每 25s 主动 ping，无需手动发 `"PING"` |
| **BTC 实时价** | `topic: "crypto_prices_chainlink"`, `filters: "{\"symbol\":\"btc/usd\"}"`（紧凑无空格字符串） | `channel: "price.crypto"`, `filter: {"symbol":"btcusd","provider":"chainlink"}`（JSON 对象） |
| **BTC 60s TWAP** | `topic: "crypto_prices_twap_sixty"`, `filters: "{\"symbol\":\"btc/usd\"}"`（`full_accuracy_value` 为 E18 整数） | `channel: "price.crypto.twap"`, `filter: {"symbol":"btcusd","window_seconds":60,"provider":"chainlink"}`（精确小数字符串，**不可再除 \(10^{18}\)**） |
| **限额与断连码** | 无显式限额 | 最多 64 订阅 / 20 帧/s；断连码：`4001` 认证拒、`4002` 消费慢、`4003` 维护、`4008` 策略违规、`4011` 缺 `provider` |

### 5.6 限流、Heartbeat 自动撤单与错误处理

1. **按 Signer 地址独立令牌桶限流**（除 Cloudflare IP 限流外独立生效）：
   - 下单桶（`POST /order` 耗 1，`POST /orders` 耗批量数）与撤单桶（`DELETE /order` 耗 1，`DELETE /cancel-all` 耗 `1 + 撤单数`）彼此独立。
   - **Standard 档**（无量门槛）：下单 `40 tokens/s`（burst `60`），撤单 `80 tokens/s`（burst `120`）；最高 **Elite（30天量 `$10M+`）**：下单 `600/s`（burst `900`），撤单 `1200/s`（burst `1800`）。
   - 批量请求满足 **All-or-Nothing**：桶内令牌不足整批需求时整批拒绝。
2. **Heartbeat 断连全撤保护（`POST /v1/heartbeats`）**：
   - 首次发送成功即激活该 API Key 的全局超时撤单保护；**必须每 5 秒发送一次**。
   - 若超过 **10 秒**未收到有效心跳，后台扫描自动触发 best-effort `cancel-all`（WebSocket keepalive **不会**刷新此计时器）。
   - 适用于挂 Maker 单的做市/策略进程防失联爆仓；停机前仍应显式调用 `DELETE /cancel-all` 并二次校验。
3. **关键错误码与重试策略**：
   - `400 Bad Request`：Tick 对齐错误、低于 `min_order_size`、余额/授权不足、`postOnly` 触发立即成交 → **不可盲目原样重试**，需重读盘口/修正价格。
   - `425 Too Early`：撮合引擎重启中 → **指数退避重试**。
   - `500 Internal Server Error`：`FOK orders are filled or killed`（深度不足）、`the trade contains rounding issues` → 按需重新拉取盘口决定是否重发。

---

## 六、链上合约清单与合规部署规范

### 6.1 Polygon Mainnet（`chainId = 137`）核心合约地址

| 类别 | 合约名称 | 地址 | 用途说明 |
| --- | --- | --- | --- |
| **抵押资产** | **pUSD** | `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB` | V2 交易与返佣抵押币（ERC-20，6 位小数） |
| **旧抵押资产** | USDC.e（Legacy） | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` | 需通过 Collateral Onramp `wrap()` 换为 pUSD |
| **CTF 账本** | Conditional Tokens (CTF) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` | CTF 结果份额 ERC-1155 合约 |
| **V2 交易所** | **CTF Exchange (V2)** | `0xE111180000d2663C0091e4f400237545B87B996B` | 标准市场 V2 撮合结算合约（需授权） |
| **V2 负风险** | **Neg Risk CTF Exchange (V2)** | `0xe2222d279d744050d28e00520010520000310F59` | NegRisk 市场 V2 撮合结算合约（需授权） |
| **V2 适配器** | Neg Risk Adapter (CTF 授权) | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` | NegRisk 市场下单前 CLOB 校验的授权目标 |
| **Split/Merge** | CtfCollateralAdapter | `0xAdA100Db00Ca00073811820692005400218FcE1f` | 标准 CTF 市场 Split / Merge / Redeem |
| **Split/Merge** | NegRiskCtfCollateralAdapter | `0xadA2005600Dec949baf300f4C6120000bDB6eAab` | NegRisk CTF 市场 Split / Merge / Redeem |
| **Protocol V2** | PositionManager Poly v2 | `0x006F54F7f9A22e0000CC2AB60031000000ae9fEF` | V2 结果份额 ERC-1155 合约 |
| **Protocol V2** | Router Poly v2 | `0x12121212006e4CD160D18e3f00711DA5c3372600` | V2 统一 Split / Merge / Redeem / Convert 入口 |
| **Protocol V2** | ExchangeV3 Poly v2 | `0xe3333700cA9d93003F00f0F71f8515005F6c00Aa` | Protocol V2 交易所合约 |
| **自动赎回** | AutoRedeemer Poly v2 | `0xa1200000d0002264C9a1698e001292D00E1b00af` | 支持 CTF 与 PositionManager 结算自动赎回 |

> **实盘授权（Allowance）清单**：实盘开单前，必须检查钱包对 `CTF Exchange (V2)`、`Neg Risk CTF Exchange (V2)`、`Neg Risk Adapter`（以及 V2 市场的 `Router`）的 **`pUSD` ERC-20 Allowance** 与 **`Conditional Tokens` `setApprovalForAll`**。

### 6.2 地理封锁（Geoblock）与 VPS 机房选址

启动前必须调用 `GET https://polymarket.com/api/geoblock` 校验 `blocked == false`：
- **完全封锁（OFAC）**：`IR, SY, CU, KP, UA-43/14/09`（无法开仓也无法平仓）。
- **API + 前端均 Close-Only（严禁用于 VPS 部署）**：`US, GB(英国), DE(德国), FR(法国), IT, PL, BE, SG(新加坡), AU, CA(BC/ON/AB/QC), TW, TH, BR, AR, RU, BY` 等。
- **仅前端 Close-Only、API 放行**：`IE(爱尔兰), NL(荷兰), JP(日本), KR(韩国), MT`。
- **最优低延迟部署方案**：
  1. **首选**：**AWS `eu-west-1`（爱尔兰都柏林）** — 官方推荐距离 `eu-west-2`（伦敦主撮合）最近的无地理封锁区域，RTT 通常 `< 12ms`。
  2. **次选**：荷兰阿姆斯特丹（`NL`）或芬兰赫尔辛基（`FI`）欧洲机房。
  3. **高阶方案**：自营交易者/做市商完成官方 KYC/KYB 后可申请直连 `eu-west-2` Co-location。

---

## 七、本项目（`poly-elder-quant`）现状核查与实盘改造清单

对照以上 2026 官方现网标准，逐项核查当前代码，发现以下 **6 项必须纳入实盘前改造计划的差异**：

| 编号 | 模块与位置 | 当前项目现状 | 2026 现网标准要求 | 优先级 |
| :---: | --- | --- | --- | :---: |
| **G1** | `requirements.txt` & `backend/execution/live_clob.py` | 依赖旧版 `py-clob-client`（CLOB V1 签名协议） | 升级 **`py-clob-client-v2`**（CLOB V2 签名结构体与 Domain v2）；**否则实盘下单必被拒** | **P0（实盘阻断）** |
| **G2** | `backend/config.py` & `backend/execution/live_clob.py` | 余额/授权查询仍指向旧 `USDC.e`（`0x2791...`）和旧 V1 Exchange（`0x4bFb...`） | 切换至 **`pUSD`**（`0xC011a...`）与 V2 Exchange（`0xE1111...` / `0xe2222...`），并兼容查询 `USDC.e` 余额提示封装 | **P0（实盘阻断）** |
| **G3** | `backend/execution/engine.py`（LIVE 分支） | 实盘提交后直接将本地订单状态写死为 `FILLED`、成交价写死为 `limit_px` | 接入 **CLOB User WebSocket（`/ws/user`）** + `GET /data/order/{id}` 回查真实成交状态、真实成交均价（Price Improvement）与实收股数 | **P0（实盘阻断）** |
| **G4** | `backend/feeds/polymarket_oracle.py`（PolyBolt） | `price.crypto` 与 `price.crypto.twap` 订阅未传 `provider` 字段 | 补齐 `"provider": "chainlink"`（或 `"binance"`），避免触发 `4011 provider_required` 错误；注意 Chainlink 源对 Chainlink API Key 的要求 | **P1（数据稳定性）** |
| **G5** | `backend/execution/live_clob.py` & `backend/feeds/polymarket_clob.py` | 未订阅 `tick_size_change`、未校验 `min_order_size`、未接 `POST /v1/heartbeats` | 接入 `tick_size` 动态精度与 `min_order_size` 校验；若启用 Maker 挂单则接入 5s Heartbeat 断连全撤保护；Data API 升级至 `/v2/positions` | **P1（实盘健壮性）** |
| **G6** | `README.md` 部署建议 | 建议部署在“德国/荷兰/芬兰” VPS | 德国（`DE`）已在 API Close-Only 封锁名单中；应修正为 **爱尔兰 `eu-west-1` / 荷兰 `NL` / 芬兰 `FI`** | **P2（部署合规）** |

> **说明**：当前处于 **模拟盘（`PAPER`）阶段** 时，公开 Gamma API、CLOB Market WS、RTDS 60s TWAP 与 `0.05` 动态 Taker 费公式均已正常工作，**G1~G3 不影响模拟盘策略验证**；在切换实盘（`LIVE`）前，务必按 **P0 → P1 → P2** 顺序完成上述改造。

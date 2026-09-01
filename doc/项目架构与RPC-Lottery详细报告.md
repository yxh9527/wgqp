# 项目架构与 RPC/Lottery 详细报告

> 依据当前工作区源码整理。重点覆盖 `lottery` 与 RPC；其他模块只保留与这两部分的依赖关系。

## 1. 结论摘要

项目是一个 Go 多模块工作区，核心运行单元如下：

| 模块 | 定位 | RPC 服务 | 默认端口 |
| --- | --- | --- | ---: |
| `lottery` | 统一资金服务：下注、奖池、预赔、结算、退款 | `lottery.LotteryService` | 10080 |
| `data-center` | 玩家/游戏/注单/哈希开奖结果查询及缓存 | `datacenter.DataCenterService` | 10081 |
| `micro_service` | proto 与生成的 Go gRPC 客户端/服务端代码 | 无独立进程 | - |
| `app` | 公共实体、配置、表模型及奖池计算依赖 | 被 `lottery`、`data-center` 引用 | - |
| `open-api`、`web-api` | 对外 HTTP/API 和后台业务 | 间接调用数据层/配置 | - |

Lottery 不生成游戏结果，也不负责游戏控制策略；游戏服负责产生下注与结果，Lottery 负责资金事实和最终账务。

## 2. 仓库结构与运行关系

```text
游戏服/网关
   │ gRPC LotteryService
   ▼
lottery
   ├─ Redis：玩家余额、奖池累计、预留汇总、牌局快照、配置、服务发现
   ├─ MySQL Player：Redis 未命中时加载玩家资料
   ├─ MySQL Manager：游戏和代理元数据
   └─ Elasticsearch：结算注单、资金流水、奖池日志

查询/后台服务 ── gRPC DataCenterService ── data-center ── Redis/MySQL/ES
                         ▲
                         └─ 两个服务都在 Redis /grpc/registry 下注册
```

工作区由 `go.work` 管理，proto 位于 `micro_service/proto`，生成代码位于 `micro_service/services`。修改 proto 后应使用 `micro_service/gen_service.bat` 重新生成并提交生成文件。

## 3. RPC 基础设施

### 3.1 服务注册与发现

Lottery 注册名为 `wg-lottery`，Data Center 注册名为 `wg-datacenter`。实际实现使用 Redis，而不是注释中多次提到的 ETCD：

```text
/grpc/registry/wg-lottery/wg-lottery-{Unix秒}
/grpc/registry/wg-datacenter/wg-datacenter-{Unix秒}
```

注册初始 TTL 为 45 秒，每 15 秒续约为 90 秒。注册值是 JSON：

```json
{"name":"wg-lottery","addr":"10.0.0.10","port":10080,"id":1710000000}
```

代码位置：[`lottery/rpc/server.go`](../lottery/rpc/server.go)、[`lottery/rpc/naming.go`](../lottery/rpc/naming.go)、[`data-center/rpc/server.go`](../data-center/rpc/server.go)。当前客户端侧没有完整的通用发现客户端；网关应自行读取 Redis 前缀、过滤过期节点并做连接池/负载均衡。

### 3.2 gRPC Server 参数

Lottery：启用 keepalive，连接最大存活 2 分钟、宽限 30 秒、心跳 30 秒、超时 15 秒。Data Center：心跳 60 秒、超时 30 秒，最大并发流 1000，收发消息上限 100 MiB。两者都没有 TLS、认证拦截器、调用方授权或审计拦截器，安全边界依赖内网和网关。

### 3.3 公共错误码

定义在 [`micro_service/proto/base.proto`](../micro_service/proto/base.proto)：

| 枚举 | 值 | 含义 |
| --- | ---: | --- |
| `OK` | 0 | 成功 |
| `GAME_FROZEN` | 1 | 游戏冻结 |
| `AGENT_FROZEN` | 2 | 代理冻结 |
| `SYSTEM_ERROR` | 3 | 系统/依赖错误 |
| `PARAMS_INVALID` | 4 | 参数或状态不满足 |
| `USER_LOGIN_ERROR` | 5 | 玩家登录错误 |
| `AUTH_PARSING_FAILED` | 6 | 认证解析失败 |
| `AUTH_TOKEN_INVALID` | 7 | 认证令牌无效 |
| `NO_ENOUGH_POOL_MONEY` | 8 | 奖池不可赔付 |
| `NO_ENOUGH_MONEY` | 9 | 玩家余额不足 |
| `ROUND_NOT_FOUND` | 10 | 牌局不存在 |

注意：业务失败通常仍返回 gRPC `err=nil`，调用方必须同时检查响应中的 `code`、`success/accepted` 和 `reason/message`。

## 4. Lottery 服务内部设计

入口为 [`lottery/cmd/run.go`](../lottery/cmd/run.go)，构造服务为 [`lottery/rpc/service.go`](../lottery/rpc/service.go)。启动顺序：日志 → YAML → Redis 与消息订阅 → `/config/*`、`/agent/*` 配置 → 两个 MySQL → ES → 游戏/代理缓存 → 奖池缓存 → 恢复 `lottery_finance_rounds` → 启动异步落地和预留回收 → gRPC 监听与注册。

### 4.1 牌局快照

`financeRound` 是每个 `roundId` 的资金事实，包含：

- 路由：`gameId`、`agent`、`level`、`symbol`、`poolSymbol`；
- 下注：`bets[betId]`，记录原币金额、CNY 金额、玩家、币种、税收、状态；
- 预留：`reservationId`、`betDigest`、`outcomeHash`、金额、到期时间、模式；
- 取消：`CancelRequests`（requestId 幂等）和 `EffectCancels`（税收/有效下注冲销）；
- 结算：`settlementId`、模式和逐玩家结果；
- 当前状态、下注修订号 `betRevision`、更新时间。

持久化为：

```text
Redis HASH lottery_finance_rounds
field = roundId
value = financeRound JSON
```

服务启动会全量恢复到内存；单实例内所有牌局共用一个 `roundLock`。

### 4.2 金额和奖池

- RPC 金额都是十进制字符串，最多两位小数；`profit` 允许负数。
- 玩家余额在 `player_{userId}` HASH 的 `currency` 字段中按“分”存储。
- 余额扣款/入账使用 Redis Lua，避免并发下余额为负。
- 汇率来自 `/config/currency`，下注和赔付换算为 CNY，奖池累计截断四位小数。
- `poolSymbol = symbol_level`，Redis 成员为 `agentId_symbol_level`。

奖池公式：

```text
E = agent_effect_data       # 累计有效下注
P = agent_profitLoss_data   # 累计实际赔付 payout
R = agent_revenue_data      # 累计税收
Q = agent_reserved_data     # 当前有效预留
BasePool      = E - P - R
AvailablePool = E - P - R - Q
```

`agent_chips_data` 只用于打码统计，不参与奖池公式。E/P/R 和玩家累计统计先在进程内聚合，每 30 秒批量覆盖 Redis；Q 通过 Redis Lua 直接原子增减。

### 4.3 状态机

```text
BETTING ── PrePay(精确/责任上限) ──> RESERVED ── Settlement(NORMAL) ──> SETTLED
   │                                  │
   │                                  ├─ 超时回收 ──> EXPIRED
   ├─ Settlement(VOID_REFUND) ────────┴───────────────────────────────> VOIDED
   └─ CancelBet：仍为 BETTING，betRevision + 1
```

`RESERVED + LIABILITY_CAP` 仍允许追加下注；精确预留禁止继续下注。`EXPIRED` 只表示 Q 已释放，不代表玩家下注已退款。

## 5. Lottery RPC 责任边界

| RPC | 资金动作 | 幂等键 | 成功后的状态 |
| --- | --- | --- | --- |
| `Bet` | 扣玩家余额，增加 E/R | `roundId + betId` | `BETTING` 或责任上限 `RESERVED` |
| `PrePay` | 增加/调整 Q | 精确模式按 `betDigest + outcomeHash` 快速幂等；责任上限复用 reservation | `RESERVED` |
| `Settlement` | 批量赔付或原路退款，更新 P/E/R/Q | `settlementId` | `SETTLED` 或 `VOIDED` |
| `GetRoundFinanceState` | 只读并触发到期检查 | `roundId` | 不改变业务意图，可能变为 `EXPIRED` |
| `CancelBet` | 退回指定玩家 ACTIVE 下注，冲销 E/R | `requestId` + 玩家/币种 | 保持 `BETTING`，修订号递增 |

详细字段、示例和调用时序见 [`Lottery-RPC对接文档.md`](./Lottery-RPC对接文档.md)。

## 6. Data Center RPC 概览

定义在 [`micro_service/proto/datacenter.proto`](../micro_service/proto/datacenter.proto)，实现于 [`data-center/rpc/service.go`](../data-center/rpc/service.go)：

| RPC | 用途 | 数据路径 |
| --- | --- | --- |
| `GetPlayer` | 玩家资料 | Redis，未命中回源 Player MySQL 并回填 |
| `GetValue` / `SetValue` | 通用键值缓存 | Redis |
| `UpdatePlayerAvatarAndGender` | 修改昵称、头像、性别 | Redis，未命中回源 MySQL |
| `GetRecords` | 查询注单 | ES `pp_gp_settlement` |
| `GetGameList` | 游戏分类和状态 | Manager MySQL |
| `UserLock` / `UserUnLock` | 玩家锁 | Redis |
| `GetSesson` | Session 读取 | Redis |
| `SaveHashLotteryResult` | 哈希开奖结果异步写入 | channel → ES `hash_lottery_result` |
| `GetHashLotteryResult` | 哈希开奖结果 | Redis 60 秒缓存 → ES |
| `GetGameRecordsList` | 游戏开奖记录 | Redis 60 秒缓存 → ES |

`SaveHashLotteryResult` 入队即返回成功，ES 批量写失败只记日志；调用方若要求可靠落库，应增加重试/补偿机制。

### 6.1 Data Center 字段速查

| RPC | 请求关键字段 | 响应关键字段/语义 |
| --- | --- | --- |
| `GetPlayer` | `playerId`、`factory` | `HumanPlayer`；Redis 未命中时回源 MySQL |
| `GetValue` | `key`、`timeOut` | `code`、`value`；读不到 key 通常仍为 `OK` 且 value 为空 |
| `SetValue` | `key`、`value`、`time_out` | `code`；TTL 单位秒 |
| `UpdatePlayerAvatarAndGender` | `PlayerId`、`Avatar`、`Gender`、`head_frame`、`name` | 空响应；更新 Redis，未命中时回源玩家库 |
| `GetRecords` | `UserId`、`Symbol`、可选 `Hash` | `RecordItem[]`；Hash 非空时按 Hash 查询 |
| `GetGameList` | 空 | `hot`、`new`、`recommend`、`all(symbol,state)` |
| `UserLock` | `userId` | `result` 字符串令牌/结果 |
| `UserUnLock` | `userId`、`token` | `result` 布尔值 |
| `GetSesson` | `key`、`timeOut` | `code`、`value`；名称保持 proto 中的历史拼写 |
| `SaveHashLotteryResult` | `gameId`、`seed`、`value` | `code`；仅入内存队列即返回 |
| `GetHashLotteryResult` | `gameId`、`seed` | `code`、`str`；Redis 60 秒缓存，未命中查 ES |
| `GetGameRecordsList` | `gameId`、`userId`、`isWinGold` | `code`、`records[]`；Redis 60 秒缓存，未命中查 ES |

Data Center 的 `GetPlayer`、`GetRecords` 等接口返回 gRPC error 的情况较多，调用方需同时处理 transport error 和响应 code；该服务没有统一的业务错误包装策略。

## 7. 已知问题与运营风险

以下是当前代码事实，不是推测：

1. **代理奖池配置键不一致。** `SetAgentPool` 使用 `agentId-symbol`，`GetPoolCfg` 查询 `agentId_symbol`，导致 `/agent/{id}/pool/{symbol}` 可能无法命中，回退到默认奖池。见 [`app/config/config.go`](../app/config/config.go)。
2. **游戏状态热更新不完整。** `GameStatusChange` 修改 `Game.State`，`resolveRuntime` 判断的是 `Game.IsFrozen`，消息更新后 RPC 可能仍接受下注。
3. **新增代理事件注册错误。** `addAgent` 注册到了 `AgentStatusChange`，而不是 `AddAgent`；触发时类型断言可能 panic。
4. **跨存储不是事务。** 余额、内存 E/P/R、Q、牌局快照和 ES 写入之间存在崩溃窗口，可能出现已扣款但无下注快照、已赔付但未 SETTLED 等情况。
5. **多实例写入受限。** `roundLock` 和 E/P/R 在进程内；同一代理必须粘到同一 Lottery 写实例，Redis 注册本身没有 fencing/CAS。
6. **服务停止不优雅。** `Server.Stop` 只记录日志，没有 `GracefulStop`，也没有退出前刷新统计和 ES 队列。
7. **异步落地可丢数据。** ES 批量失败后当前批次被清空；统计写失败时 `UpdateTime` 已清零。
8. **协议与实现有差异。** `reservedItems` 当前始终为空；`reservationMode` 除 `LIABILITY_CAP` 外没有白名单校验；无续期、主动释放、部分退款和自动补偿。
9. **缺少服务内认证。** 任何能访问 gRPC 端口的调用方都可尝试资金操作，必须在网络层或网关层隔离。
10. **配置包含明文凭据。** `lottery/config.yaml`、`data-center/config.yaml` 当前直接包含 MySQL/ES 密码；应迁移到密钥管理或环境注入，并轮换已暴露凭据。
11. **服务注册 ID 只有 Unix 秒。** 同一服务多实例在同一秒启动时可能生成相同注册键，造成覆盖；应改为纳秒/UUID，并在客户端实现节点去重。

## 8. 建议优先级

### P0：资金正确性

- 修复代理奖池键和游戏冻结字段；修复 `addAgent` 注册。
- 为 Bet/PrePay/Settlement 引入持久化幂等记录或 Redis Lua 状态机，缩小跨存储崩溃窗口。
- 结算采用可恢复的事务日志/状态阶段，避免“已入账但重试重复入账”。
- 补充多实例 fencing token 或强制代理粘性路由。

### P1：可运维性

- `GracefulStop`、队列 drain、ES/统计失败重试队列和告警指标。
- 牌局快照版本号/CAS、归档和 TTL 策略。
- 增加全链路 requestId、settlementId、reservationId 日志和指标。

### P2：协议完善

- 为 `reservationMode`、状态和 reason 建立明确枚举；实现 `reservedItems` 或从 proto 删除。
- 增加预留续期/释放（如业务需要）、部分退款和故障自动补偿前的人工审批接口。
- gRPC TLS、服务身份和方法级授权。

## 9. 验证基线

已执行：

```text
go test ./lottery/... ./data-center/... ./micro_service/...
```

结果：全部通过。现有测试重点覆盖金额解析、奖池标识、下注摘要、CancelBet 幂等/回滚和修订号；尚未覆盖完整 PrePay、Settlement、超时回收、跨存储故障注入和多实例竞争。

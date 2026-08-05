# 统一 RPC 协议草案

> 文档状态：与当前 `lottery` 实现同步。接口仍处于草案阶段，但字段、状态和资金口径以当前代码为准。

## 1. 目标与范围

统一资金服务只处理真人玩家的下注、奖池预留、结算、退款和资金状态查询，不生成游戏结果，也不负责游戏控制策略。

当前提供四个 RPC：

1. `Bet`：扣除玩家余额并登记本局下注。
2. `PrePay`：按冻结后的下注集合和候选结果申请赔付预留。
3. `Settlement`：正常赔付或整局全额退款。
4. `GetRoundFinanceState`：查询牌局资金状态，用于超时确认、故障后的状态核对和人工审核。

赔付预留只用于单人房游戏，默认有效期为 300 秒，不提供续期和主动释放接口。有效预留只能通过 `Settlement(NORMAL)`、`Settlement(VOID_REFUND)` 或到期自动回收结束。

游戏服务发生故障后，不自动继续旧牌局。恢复后的游戏必须生成新的 `roundId`；旧牌局资金状态保留，由人工审核后决定是否调用 `VOID_REFUND`。

## 2. 核心标识

- `roundId`：牌局全局唯一编号，也是 `lottery_finance_rounds` 的字段键。因为状态查询只接收 `roundId`，不同代理、游戏和实例之间都不能重复。
- `betId`：本局内单笔下注的唯一编号。`roundId + betId` 是下注幂等键。
- `requestId`：调用请求编号。当前 `Bet` 只校验非空，不使用它实现扣款幂等；下注幂等由 `betId` 保证。
- `betDigest`：本局全部已接受下注的 SHA-256 摘要，用于锁定参与预赔和结算的订单集合。
- `outcomeHash`：完整候选游戏结果的摘要，用于锁定预赔对应的结果。
- `reservationId`：一次赔付预留的唯一编号。
- `settlementId`：最终结算编号，也是结算幂等键。

已删除实例作用域和所有权令牌字段。游戏故障后不迁移旧牌局，旧数据进入人工审核流程。

## 3. 金额与币种

- RPC 中的金额使用十进制字符串。
- 玩家账户余额在 Redis 中以整数“分”为单位保存。
- `BetItem.amount`、`PrePayItem.payout`、`SettlementItem.betAmount`、`SettlementItem.payout`、`SettlementItem.profit` 和 `SettlementItem.pump` 最多保留两位小数。
- 下注、赔付和税收按 `currencyType` 对应汇率换算为 CNY 后计入奖池。
- CNY 奖池累计值截断到四位小数。
- `currencyType` 不参与奖池键；不同币种换算后共同进入同一个 CNY 奖池。
- 禁止使用 JavaScript 浮点数直接参与资金计算。调用方应始终传递十进制字符串。

## 4. 奖池数据

奖池按 `agentId + symbol + level` 隔离。代码内部先构造：

```text
poolSymbol = symbol_level
redisMember = agentId_symbol_level
```

例如代理 `12`、游戏 `baccarat`、房间等级 `2`：

```text
poolSymbol = baccarat_2
redisMember = 12_baccarat_2
```

Redis 数据结构：

| Redis Key                  | 类型 | 含义                                                              |
| -------------------------- | ---- | ----------------------------------------------------------------- |
| `agent_effect_data`      | ZSET | 代理累计有效下注 `E`，金额为 CNY                                |
| `agent_profitLoss_data`  | ZSET | 代理累计总赔付 `P`，直接累加正常结算时玩家实际收到的 `payout` |
| `agent_revenue_data`     | ZSET | 代理累计税收 `R`，下注时计提                                    |
| `agent_chips_data`       | ZSET | 代理累计打码统计，不参与奖池公式                                  |
| `agent_reserved_data`    | ZSET | 当前有效预留总额 `Q`                                            |
| `lottery_finance_rounds` | HASH | 每个 `roundId` 的下注、预留、结算和状态快照                     |

`agent_profitLoss_data` 不是代理净盈亏。它只记录累计总赔付，不混入下注金额，也不记录 `payout - betAmount`。

当前奖池公式：

```text
BasePool = E - P - R
AvailablePool = E - P - R - Q
```

资金变化：

| 操作                        |   E 有效下注 |         P 总赔付 |       R 税收 |       Q 有效预留 |
| --------------------------- | -----------: | ---------------: | -----------: | ---------------: |
| `Bet` 成功                | 增加下注 CNY |             不变 | 增加下注税收 |             不变 |
| `PrePay` 成功             |         不变 |             不变 |         不变 | 增加预计赔付 CNY |
| 超时自动回收              |         不变 |             不变 |         不变 |     减少对应预留 |
| `Settlement(NORMAL)`      |         不变 | 增加实际赔付 CNY |         不变 |     释放整笔预留 |
| `Settlement(VOID_REFUND)` | 撤销本局下注 |             不变 | 撤销本局税收 |     释放整笔预留 |

实际赔付小于预留时，结算会释放整笔预留，再把实际赔付累加到 `P`，差额自然回到可用奖池。实际赔付大于预留时拒绝结算。

## 5. 牌局状态

当前状态：

```text
BETTING   允许首次下注和追加下注
RESERVED  已建立有效赔付预留
SETTLED   已完成正常结算
VOIDED    已完成整局全额退款
EXPIRED   预留已超时释放，下注尚未退款
```

主要状态流转：

```text
BETTING -> RESERVED -> SETTLED
                    -> EXPIRED
                    -> VOIDED

BETTING  -> VOIDED
EXPIRED  -> RESERVED 或 VOIDED
```

`EXPIRED` 只表示预留已经自动释放，不代表下注已经退回。只有 `Settlement(mode=VOID_REFUND)` 才会退还下注并进入 `VOIDED`。

## 6. Bet

### 6.1 请求

```ts
interface BetItem {
  betId: string;
  userId: number;
  currencyType: string;
  amount: string;
  areaId?: string;
}

interface BetRequest {
  requestId: string;
  roundId: string;
  gameId: number;
  agent: number;
  level: number;
  items: BetItem[];
}
```

`BetRequest` 的历史字段号 `6、7` 已保留，禁止复用；`items` 使用字段号 `8`。

### 6.2 行为

- 校验代理、游戏和奖池配置。
- 首次下注创建 `BETTING` 牌局。
- 已存在牌局必须与原 `agent + gameId + level + poolSymbol` 一致。
- 只有 `BETTING` 状态接受新下注。
- 玩家余额扣减通过 Redis Lua 原子执行，余额不能扣成负数。
- 下注金额换算为 CNY 后增加 `agent_effect_data`。
- 税收按 `下注 CNY * 税率` 计入 `agent_revenue_data`。
- 每笔已接受下注参与 `betDigest` 计算。
- 不生成游戏结果、不创建预留、不执行结算。

### 6.3 幂等与批量语义

- 相同 `roundId + betId` 重试返回首次成功结果，不重复扣款。
- 已成功下注会持久化，失败项不会写入牌局下注快照。
- 批量请求逐项处理；某项失败不会回滚此前成功项。
- `BetResponse.code` 表示请求级错误；单项结果通过 `BetItemResult.code` 和 `accepted` 返回。

`betDigest` 的当前计算规则为：筛选全部 `accepted=true` 的下注，按 `betId` 排序，将 `betId、userId、currencyType、规范化 amount、areaId` 拼接后计算 SHA-256 十六进制字符串。

## 7. PrePay

### 7.1 请求与响应

```ts
interface PrePayItem {
  userId: number;
  currencyType: string;
  payout: string;
}

interface PrePayRequest {
  requestId: string;
  roundId: string;
  gameId: number;
  agent: number;
  level: number;
  betDigest: string;
  outcomeHash: string;
  timeoutSeconds: number;
  items: PrePayItem[];
}

interface PrePayResponse {
  code: number;
  success: boolean;
  roundId: string;
  state: string;
  reservationId: string;
  totalPayoutCny: string;
  expiresAt: number;
  reason: string;
  betDigest: string;
  outcomeHash: string;
}
```

### 7.2 行为

- `betDigest` 必须与资金服务重新计算的摘要完全一致。
- `outcomeHash` 必须非空。
- `items` 必须按 `userId + currencyType` 覆盖本局全部已接受下注账户，不能遗漏、增加或重复。
- 同一玩家同一币种的多笔下注，在预赔时合并为一个账户项。
- `payout` 是实际返还玩家的总金额，不是净利润。
- 所有 `payout` 按币种汇率换算为 CNY 后汇总为 `totalPayoutCny`。
- Redis Lua 使用 `BasePool - 当前有效预留 >= 本次预留` 判断是否可预留，并在成功时原子增加 `agent_reserved_data`。
- `timeoutSeconds=0` 时使用默认值 300 秒。该字段为兼容现有协议保留，单人房游戏应传 `0`，由资金服务统一使用默认超时。
- 成功后牌局进入 `RESERVED`，不再接受下注。

### 7.3 幂等与冲突

- 牌局已经存在有效预留，且 `betDigest + outcomeHash` 一致时，直接返回原 `reservationId`。
- 已存在有效预留但 `outcomeHash` 不同，返回 `ROUND_CONFLICT`。有效期内不允许切换候选结果；只能完成结算、执行整局退款，或等待预留超时后再申请新候选。
- `SETTLED` 和 `VOIDED` 不允许再次预留。
- `EXPIRED` 可以重新调用 `PrePay` 建立新预留。

`reason` 当前可能为：

```text
INVALID_REQUEST
BET_MISMATCH
ROUND_CONFLICT
INSUFFICIENT_POOL
RESERVATION_PERSIST_FAILED
```

## 8. Settlement

### 8.1 请求

```ts
type SettlementMode = "NORMAL" | "VOID_REFUND";

interface SettlementItem {
  userId: number;
  currencyType: string;
  betAmount: string;
  payout: string;
  profit: string;
  record: string;
  pump: string;
}

interface SettlementRequest {
  settlementId: string;
  roundId: string;
  gameId: number;
  agent: number;
  level: number;
  reservationId: string;
  betDigest: string;
  outcomeHash: string;
  mode: SettlementMode;
  items: SettlementItem[];
}
```

`mode` 为空时按 `NORMAL` 处理。

### 8.2 共同校验

- `settlementId` 必须非空。
- `betDigest` 必须与资金服务计算结果完全一致。
- `items` 必须按 `userId + currencyType` 覆盖本局全部已接受下注账户，不能遗漏或重复。
- `betAmount` 必须等于该账户在本局全部已接受下注的合计。
- `profit` 必须等于 `payout - betAmount`，允许为负数。
- `pump` 是游戏侧计算的玩家币种抽水金额，必须为非负金额且最多保留两位小数。
- 真人玩家输且 `payout=0` 时仍必须提交结算项。
- 所有请求项通过校验后才开始批量修改玩家余额。

### 8.3 NORMAL

- 必须存在状态为 `RESERVED` 的有效预留。
- `reservationId` 和 `outcomeHash` 必须与预留完全一致。
- 实际赔付 CNY 总额不能超过预留总额。
- 将各玩家 `payout` 加入玩家余额。
- 将实际赔付 CNY 累加到 `agent_profitLoss_data`。
- 注单同时保存原币种抽水 `pmup`，以及按汇率换算为 CNY 的抽水 `exPmup`；抽水不参与当前奖池公式。
- 释放整笔预留，状态进入 `SETTLED`。
- 实际赔付小于预留时，差额因整笔预留释放而自动回到可用奖池。

### 8.4 VOID_REFUND

- 目前只支持整局全额退款，不支持部分退款。
- 每个账户必须满足 `payout = betAmount` 和 `profit = 0`。
- 每个账户的 `pump` 必须为 `0`。
- `payout` 为空时，服务自动使用 `betAmount`。
- 如果存在有效预留，必须提交匹配的 `reservationId + outcomeHash`。
- 将下注原路退回玩家余额。
- 从 `agent_effect_data` 撤销本局下注，并从 `agent_revenue_data` 撤销对应税收。
- 不增加 `agent_profitLoss_data`，因为退款不是游戏赔付。
- 如果存在有效预留，同时释放该预留。
- 最终状态进入 `VOIDED`。

### 8.5 幂等

- 相同 `settlementId` 重试返回首次保存的结算结果，不重复入账。
- 牌局已 `SETTLED` 或 `VOIDED` 后使用其他 `settlementId`，返回参数错误。
- RPC 超时后必须先调用 `GetRoundFinanceState`，不能直接更换 `settlementId` 重试。

## 9. GetRoundFinanceState

```ts
interface GetRoundFinanceStateRequest {
  roundId: string;
}

interface GetRoundFinanceStateResponse {
  code: number;
  roundId: string;
  state: string;
  betDigest: string;
  reservationId: string;
  outcomeHash: string;
  expiresAt: number;
  settlementId: string;
  totalBetCny: string;
  totalReservedCny: string;
}
```

- 查询前会先执行预留过期检查。
- `totalBetCny` 是本局全部已接受下注换算后的 CNY 合计。
- 只有预留仍处于 `RESERVED` 时，`totalReservedCny` 返回预留金额；其他状态返回 `0`。
- 牌局不存在时当前返回 `PARAMS_INVALID`。

## 10. 预留超时与回收

- 服务每 5 秒扫描一次内存中的牌局。
- `expiresAt` 使用 Unix 秒。
- 到期后先通过 Redis Lua 释放 `agent_reserved_data`，成功后再把预留和牌局状态改为 `EXPIRED`。
- Redis 释放失败时状态保持 `RESERVED`，后续扫描继续重试。
- `Bet`、`PrePay`、`Settlement` 和 `GetRoundFinanceState` 处理牌局时也会执行到期检查。
- 服务启动时从 `lottery_finance_rounds` 恢复牌局，已过期预留会在下一轮扫描或下一次 RPC 访问时回收。
- 正常运行时，预留通常会在到期后的 5 秒内回收。

预留释放只恢复可用奖池，不自动退还玩家下注。超时牌局进入 `EXPIRED` 后，应由游戏重新预赔，或由人工审核后执行整局退款。

## 11. Redis 持久化与恢复

牌局状态保存在：

```text
HASH lottery_finance_rounds
field = roundId
value = financeRound JSON
```

快照包含：

- `agent、gameId、level、symbol、poolSymbol`
- 已接受的下注快照
- 当前或历史预留凭证
- 最终结算结果
- 当前状态和更新时间

`agent_effect_data`、`agent_profitLoss_data`、`agent_revenue_data` 和 `agent_chips_data` 当前在 lottery 进程内缓存修改，每 30 秒批量写入 Redis。`agent_reserved_data` 在预留生命周期操作中直接通过 Redis Lua 更新。

## 12. 调用顺序

正常游戏：

```text
首次或追加下注 -> Bet
  -> 封盘并冻结订单
  -> 生成完整候选结果和 outcomeHash
  -> PrePay
  -> 固定并公开结果
  -> Settlement(NORMAL)
```

有效预留期间不支持切换候选结果。若确实需要切换，只能等待预留超时进入 `EXPIRED` 后重新调用 `PrePay`；若牌局已经故障，则保留旧牌局数据并进入人工审核流程。

游戏未成立或故障后人工确认退款：

```text
GetRoundFinanceState
  -> 人工审核
  -> Settlement(VOID_REFUND)
```

游戏进程故障恢复后必须创建新的 `roundId`，不自动接续旧牌局。旧牌局保留给人工审核。

## 13. 当前部署约束

- 玩家余额扣款使用 Redis Lua，单个玩家扣款不会因并发而变成负数。
- 预留汇总的检查和增加使用 Redis Lua，相同奖池的并发预留不会只依赖进程内计数。
- `roundLock` 是进程内锁，`lottery_finance_rounds` 当前没有跨实例 CAS。
- 基础奖池 `E/P/R` 当前来自实例内缓存，并每 30 秒异步落 Redis。
- 因此同一代理的写请求应路由到同一个 lottery 写实例。按代理分片可以横向扩展，但同一代理不能同时由多个实例写入。

推荐路由键：

```text
hash(agentId) -> lottery instance
```

`GetRoundFinanceState` 请求中没有 `agent` 字段，网关若按代理路由，需要维护 `roundId -> agentId` 路由映射，或先查询共享状态后转发。

## 14. 当前一致性边界

以下操作目前不是一个完整的跨存储事务：

1. 玩家余额修改、奖池缓存修改、牌局快照写入和 ES 注单/流水落地。
2. `PrePay` 增加 `agent_reserved_data` 与保存 `RESERVED` 牌局快照。
3. 超时或结算时减少 `agent_reserved_data` 与保存最终牌局状态。
4. 批量结算多个玩家余额与最终牌局状态保存。

如果进程在上述步骤之间崩溃，可能出现余额已变更但状态未落库、预留汇总与牌局状态不一致，或 ES 明细延迟/失败。当前处理方式是保留牌局和资金数据，由人工审核；系统不会在不确定时自动退款。

`agent_reserved_data` 目前只保存每个奖池的汇总预留金额，不保存每个 `reservationId` 的独立账本。每笔预留详情保存在 `lottery_finance_rounds`。严格的崩溃一致性仍需要把“按 reservationId 去重、更新汇总预留、更新牌局状态”合并为同一个 Redis 原子操作。

## 15. 当前未实现能力

- 部分退款。
- 实际赔付超过预留时的追加预留。
- 跨 Redis、数据库和 ES 的分布式事务或自动补偿。
- 多实例并发修改同一个代理时的 fencing token/CAS。
- 按 `reservationId` 独立保存的预留账本。
- 故障牌局自动续局或自动退款。
- 已完成牌局状态的自动归档和清理。

上述场景在实现前必须先明确幂等键、状态转换和人工审核边界。

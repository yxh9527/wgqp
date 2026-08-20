# Lottery 当前功能与流程

> 文档基线：2026-08-17 当前工作区代码。本文描述已经实现的行为，不代表未来设计目标。

## 1. 服务定位

`lottery` 当前是统一资金服务，不负责生成游戏结果，也不负责开奖结果控制。它负责：

1. 扣除真人玩家下注余额。
2. 维护牌局资金状态和下注幂等记录。
3. 按代理、游戏和房间等级维护奖池累计值。
4. 在正常结算前建立限时赔付预留。
5. 批量发放赔付，或对故障牌局执行整局退款。
6. 自动回收超时预留。
7. 写入资金流水、结算注单和奖池变化日志。
8. 提供牌局资金状态查询，支持 RPC 超时确认和人工审核。

当前服务对外只提供四个 gRPC：

| RPC | 作用 |
| --- | --- |
| `Bet` | 扣除玩家余额并记录本局下注 |
| `PrePay` | 建立或调整赔付预留 |
| `Settlement` | 正常结算或整局退款 |
| `GetRoundFinanceState` | 查询牌局资金状态 |

明确未提供 `RenewPrePay` 和 `ReleasePrePay`。预留只能由结算、整局退款或超时回收释放。

## 2. 依赖与启动流程

### 2.1 外部依赖

| 组件 | 当前用途 |
| --- | --- |
| Redis | 玩家余额、奖池累计值、有效预留、牌局快照、配置、服务注册和事件广播 |
| MySQL Player | Redis 玩家缓存未命中时读取玩家资料 |
| MySQL Manager | 启动时加载游戏和代理配置 |
| Elasticsearch | 保存结算注单、资金流水和奖池日志 |
| gRPC | 对游戏服务提供统一资金接口 |

### 2.2 启动顺序

执行 `lottery run --config ./config.yaml` 后，当前顺序为：

```text
初始化日志
  -> 读取 YAML 基础配置
  -> 创建 Redis 客户端并订阅 message 频道
  -> 从 Redis 加载系统、币种、奖池和代理奖池配置
  -> 连接玩家库和管理库
  -> 连接 Elasticsearch
  -> 从管理库加载游戏和代理
  -> 初始化奖池内存缓存
  -> 从 lottery_finance_rounds 恢复全部牌局快照
  -> 启动预留超时扫描和异步落地协程
  -> 启动 gRPC 服务
  -> 在 Redis 注册 wg-lottery 服务地址
```

服务注册键格式为：

```text
/grpc/registry/wg-lottery/wg-lottery-{Unix秒}
```

初始 TTL 为 45 秒，后台每 15 秒续约为 90 秒。代码注释写的是“ETCD”，实际实现使用 Redis。

### 2.3 后台任务

| 周期或阈值 | 任务 |
| --- | --- |
| 每 5 秒 | 扫描并回收超时预留 |
| 每 30 秒 | 将内存中的代理和玩家累计统计批量写入 Redis |
| 每 5 秒 | 汇总奖池日志，达到 32 条或每 5 秒批量写 ES |
| 每 5 秒 | 扫描结算注单缓存并投递到写入队列 |
| 达到 40 条或每 10 秒 | 批量写入结算注单 |
| 达到 50 条或每 10 秒 | 批量写入资金流水 |
| 每 300 秒 | 从管理库重新加载代理列表 |

## 3. 核心标识与金额口径

### 3.1 核心标识

| 标识 | 含义和约束 |
| --- | --- |
| `roundId` | 全局唯一牌局编号，也是 Redis 牌局快照字段键 |
| `betId` | 本局内的单笔下注编号；`roundId + betId` 构成下注幂等键 |
| `requestId` | 请求跟踪编号；当前只要求非空，不单独提供完整幂等保证 |
| `betDigest` | 本局全部已接受下注的 SHA-256 摘要 |
| `outcomeHash` | 游戏侧候选结果或最终结果摘要 |
| `reservationId` | 一次预留的唯一编号 |
| `settlementId` | 最终结算幂等编号 |

`roundId` 一旦创建，会绑定原始 `agent + gameId + level + poolSymbol`。后续请求不匹配时返回参数错误，防止跨代理、跨游戏或跨等级复用牌局。

### 3.2 金额规则

1. RPC 金额使用十进制字符串。
2. 玩家 Redis 余额以整数“分”为单位。
3. 下注、预赔、赔付、利润和抽水最多保留两位小数。
4. 玩家金额按 `currencyType` 配置汇率换算为 CNY 后参与奖池。
5. CNY 奖池累计值截断到四位小数。
6. `SettlementItem.profit` 可以为负数，其他金额必须非负。
7. 游戏服务不能使用浮点数直接生成资金字符串。

### 3.3 下注摘要

`betDigest` 的计算规则：

```text
筛选 accepted=true 的下注
  -> 按 betId 排序
  -> 每项拼接 betId|userId|currencyType|amount|areaId
  -> 各项使用换行连接
  -> SHA-256
  -> 十六进制字符串
```

## 4. 奖池与统计

### 4.1 隔离维度

奖池按 `agentId + game symbol + level` 隔离：

```text
poolSymbol = symbol_level
redisMember = agentId_symbol_level
```

例如代理 `12`、游戏 `slot_a`、等级 `2`：

```text
poolSymbol = slot_a_2
redisMember = 12_slot_a_2
```

### 4.2 奖池公式

定义：

```text
E = TotalEffectBet，代理累计有效下注 CNY
P = TotalProfLoss，代理累计总赔付 CNY
R = TotalRevenue，代理累计税收 CNY
Q = 当前有效预留总额 CNY
```

当前公式：

```text
BasePool = E - P - R
AvailablePool = E - P - R - Q
```

`TotalProfLoss` 当前不是代理净盈亏，而是正常结算时玩家实际收到的 `payout` 累计值。

### 4.3 资金变化

| 操作 | E | P | R | Q |
| --- | ---: | ---: | ---: | ---: |
| `Bet` 成功 | 增加下注 CNY | 不变 | 增加下注税收 | 不变 |
| `PrePay` 首次成功 | 不变 | 不变 | 不变 | 增加预留 CNY |
| `PrePay` 调整责任上限 | 不变 | 不变 | 不变 | 按新旧差额增加或减少 |
| `Settlement(NORMAL)` | 不变 | 增加实际赔付 CNY | 不变 | 释放整笔预留 |
| `Settlement(VOID_REFUND)` | 撤销本局下注 | 不变 | 撤销本局税收 | 释放整笔预留 |
| 预留超时 | 不变 | 不变 | 不变 | 释放整笔预留 |

抽水 `pump` 只写入结算注单，不参与当前奖池公式。

### 4.4 Redis 统计键

| Redis Key | 类型 | 内容 |
| --- | --- | --- |
| `agent_effect_data` | ZSET | 代理奖池累计有效下注 E |
| `agent_profitLoss_data` | ZSET | 代理奖池累计总赔付 P |
| `agent_revenue_data` | ZSET | 代理奖池累计税收 R |
| `agent_chips_data` | ZSET | 代理累计打码量 |
| `agent_reserved_data` | ZSET | 当前有效预留汇总 Q |
| `userTotalEffBet` | ZSET | 玩家累计有效下注 |
| `userTotalProfLoss` | ZSET | 玩家累计总返奖 |
| `userBetCount` | ZSET | 玩家累计结算局数 |

代理和玩家统计先写实例内存，每 30 秒覆盖写入 Redis。`agent_reserved_data` 直接通过 Redis Lua 原子更新。

游客玩家仍会发生余额扣款、赔付和流水写入，但 `ApplyPoolChange` 与 `RecordSettlement` 会跳过游客，因此不计入上述奖池和玩家累计统计。

## 5. 牌局状态机

### 5.1 状态

| 状态 | 含义 |
| --- | --- |
| `BETTING` | 可接受下注 |
| `RESERVED` | 已建立有效预留 |
| `SETTLED` | 已完成正常结算 |
| `VOIDED` | 已完成整局退款 |
| `EXPIRED` | 预留已超时释放，但下注没有自动退款 |

`LIABILITY_CAP` 是预留模式，不是牌局状态。

### 5.2 状态流转

```text
新 roundId
  -> Bet
  -> BETTING

BETTING
  -> PrePay(精确预留)
  -> RESERVED，禁止继续下注

BETTING
  -> PrePay(LIABILITY_CAP)
  -> RESERVED，仍允许继续下注
      -> Bet 可重复追加
      -> PrePay(LIABILITY_CAP) 调整上限
      -> PrePay(精确预留) 转为精确结果预留并停止下注

RESERVED
  -> Settlement(NORMAL)
  -> SETTLED

BETTING / RESERVED / EXPIRED
  -> Settlement(VOID_REFUND)
  -> VOIDED

RESERVED
  -> 到期自动回收
  -> EXPIRED

EXPIRED
  -> PrePay
  -> RESERVED
```

`EXPIRED` 不代表玩家已退款，并且不能继续调用 `Bet`。它可以重新预赔，或由人工审核后调用整局退款。

## 6. Bet 流程

### 6.1 请求级校验

`Bet` 要求：

1. `requestId`、`roundId` 非空。
2. `gameId`、`agent` 非零。
3. `items` 非空。
4. 代理存在且未冻结。
5. 游戏存在且未冻结。
6. 代理或默认奖池配置存在。
7. 已存在牌局的代理、游戏、等级和奖池标识必须一致。

### 6.2 单项处理

每个 `BetItem` 按顺序独立处理：

```text
检查 roundId + betId 是否已有成功快照
  -> 有：直接返回原结果
  -> 无：继续

检查牌局是否允许下注
  -> BETTING：允许
  -> RESERVED + LIABILITY_CAP：允许
  -> 其他状态：拒绝该项

校验 betId、userId、currencyType 和正数 amount
  -> 获取汇率
  -> Redis Lua 原子扣除玩家余额
  -> 换算下注 CNY
  -> 保存下注快照
  -> E 增加下注 CNY
  -> R 增加下注 CNY * 税率
  -> 生成负数下注流水
  -> 更新奖池日志待写队列
```

玩家 Redis 缓存不存在时，服务会从玩家 MySQL 读取完整资料，写入 Redis 后重试一次扣款。玩家 Hash 默认续期 20 分钟，余额修改后加入 `dirty_list_imp`，由外部流程持久化玩家余额。

### 6.3 批量和幂等语义

1. 批量下注不是全有或全无，前面成功项不会因后续失败回滚。
2. 只有成功下注会写入牌局快照；失败项只在本次响应中返回。
3. 相同 `roundId + betId` 返回首次成功结果，不重复扣款。
4. 重复 `betId` 即使请求中的玩家或金额不同，也会直接返回原快照。
5. `BetResponse.code` 是请求级错误；单项结果看 `BetItemResult.code` 和 `accepted`。
6. 责任上限预留下追加下注后，会同步更新预留记录中的 `betDigest`，但不会自动增加预留金额。

## 7. PrePay 流程

### 7.1 通用校验

`PrePay` 必须：

1. 指向已经存在且至少有一笔成功下注的牌局。
2. 携带当前完整 `betDigest`。
3. 携带非空 `outcomeHash`。
4. `items` 按 `userId + currencyType` 完整覆盖全部已下注账户。
5. 每个账户只出现一次，`payout` 为非负金额。
6. 所有赔付按汇率换算为 CNY 后汇总为 `totalPayoutCny`。

同一玩家同一币种的多笔下注在预赔时必须合并为一个账户项。

### 7.2 精确结果预留

`reservationMode` 为空时，按精确结果预留处理：

```text
冻结当前 betDigest 和 outcomeHash
  -> Redis Lua 检查 BasePool - 当前 Q >= 本次预留
  -> 成功后增加 agent_reserved_data
  -> 生成 reservationId
  -> 保存到期时间
  -> 牌局进入 RESERVED
  -> 禁止继续下注
```

正常结算时必须使用相同的 `reservationId` 和 `outcomeHash`。

已有精确预留时：

1. 当前 `betDigest + outcomeHash` 一致，直接返回原预留。
2. 不一致时返回 `ROUND_CONFLICT`。
3. 有效期内不能主动释放或切换精确结果。

### 7.3 责任上限预留

`reservationMode=LIABILITY_CAP` 表示责任上限预留，主要用于单人房发牌前先锁定最大赔付责任：

1. 牌局状态仍为 `RESERVED`，但允许继续调用 `Bet`。
2. 后续下注会更新 `betDigest`，不会自动扩大预留上限。
3. 可以再次调用 `PrePay(LIABILITY_CAP)` 调整预留：
   - 新上限大于旧上限：只对差额执行追加预留。
   - 新上限小于旧上限：只释放差额。
   - 保持相同 `reservationId`，刷新摘要、结果、金额和到期时间。
4. 也可以再次调用 `PrePay` 并将模式改为空，转换为精确结果预留；转换后禁止继续下注。
5. 直接正常结算时，最终 `outcomeHash` 可以与预留时不同，但实际总赔付不能超过责任上限。

当前代码只对字符串 `LIABILITY_CAP` 启用特殊行为，其他非空字符串没有被显式拒绝，会被保存但按精确预留行为处理。

### 7.4 超时

1. `timeoutSeconds=0` 使用默认 300 秒。
2. 非零值由调用方覆盖默认值，当前没有服务端最大值限制。
3. 服务没有续期 RPC；责任上限调整会重新计算到期时间。
4. 超时回收失败时保持 `RESERVED`，5 秒后的扫描继续重试。

### 7.5 预留幂等边界

当前精确预留的重复判断主要使用 `betDigest + outcomeHash`，不是 `requestId`。如果摘要和结果相同，即使再次提交的赔付明细不同，也会返回原预留，不会重新计算金额。

## 8. Settlement 流程

`mode` 支持：

```text
NORMAL       正常赔付
VOID_REFUND  整局全额退款
```

`mode` 为空时按 `NORMAL` 处理。

### 8.1 通用校验

1. `settlementId`、`roundId`、`gameId`、`agent` 和 `items` 必须有效。
2. 代理、游戏、等级和奖池必须与原牌局一致。
3. `betDigest` 必须等于当前完整下注摘要。
4. `items` 必须按 `userId + currencyType` 完整覆盖全部下注账户。
5. `betAmount` 必须等于该账户本局成功下注合计。
6. `profit` 必须等于 `payout - betAmount`；为空时服务自动计算。
7. `pump` 必须是非负金额；退款模式必须为 `0`。
8. 所有请求项先完成校验，再开始修改玩家余额。

### 8.2 正常结算

正常结算要求存在有效预留：

```text
校验 reservationId
  -> 精确预留校验 outcomeHash 完全一致
  -> 责任上限预留允许最终 outcomeHash 不同
  -> 汇总实际 payout CNY
  -> 校验实际赔付 <= 预留金额
  -> Redis Pipeline 批量增加玩家余额
  -> 生成结算注单和正数赔付流水
  -> P 增加实际赔付 CNY
  -> 更新打码量、玩家局数和玩家返奖累计
  -> 释放整笔 Q
  -> 保存 SETTLED 状态和 settlementId
```

打码量当前取 `max(betAmount, payout)`。

结算注单同时保存：

1. 原币种下注、赔付、税收和抽水。
2. 换算 CNY 后的 `exBet`、`exWin`、`exRevenue` 和 `exPmup`。
3. 游戏侧传入的完整 `record` 字符串。
4. 结算后余额、游戏和代理信息。

注单 ID 使用 `MD5(agentId|userId|roundId)`，同一玩家同一牌局写入同一个 ES 文档。

### 8.3 整局退款

退款模式当前只支持整局退款：

1. 每个账户满足 `payout = betAmount`。
2. `profit = 0`；为空时可自动计算。
3. `pump = 0`。
4. 如果存在有效预留，必须匹配 `reservationId`；精确预留还必须匹配 `outcomeHash`。
5. 如果预留已经超时，仍可按下注快照直接执行退款。
6. 下注金额原路增加回玩家余额。
7. E 撤销本局下注，R 撤销本局税收，P 不增加。
8. 有效预留被释放，牌局进入 `VOIDED`。

退款也会生成结算注单和正数 `void_refund` 资金流水。

### 8.4 结算幂等

牌局已经保存相同 `settlementId` 时，直接返回首次保存的结果，不重复入账。牌局已经 `SETTLED` 或 `VOIDED` 后使用其他 `settlementId` 会被拒绝。

该幂等只在最终牌局快照成功保存后成立；跨存储中途崩溃的边界见第 14 节。

## 9. GetRoundFinanceState 流程

查询只需要 `roundId`：

```text
从内存查找
  -> 未命中时从 lottery_finance_rounds 读取
  -> 先执行预留到期检查
  -> 返回当前状态和资金摘要
```

当前实际返回：

| 字段 | 含义 |
| --- | --- |
| `state` | 当前牌局状态 |
| `betDigest` | 当前完整下注摘要 |
| `totalBetCny` | 全部成功下注 CNY 合计 |
| `reservationId` | 当前或历史预留编号 |
| `outcomeHash` | 当前或历史预留结果摘要 |
| `expiresAt` | 预留到期 Unix 秒 |
| `reservationMode` | 空或 `LIABILITY_CAP` |
| `totalReservedCny` | 仅有效预留返回金额，否则返回 `0` |
| `settlementId` | 已结算时的结算编号 |

协议中还定义了 `reservedItems`，但当前服务没有保存预留明细，也没有给该字段赋值，因此实际始终为空。

查询用于 RPC 超时确认、故障后的资金核对和人工审核。游戏进程故障恢复后应创建新 `roundId`，服务不会自动继续旧游戏局。

## 10. 预留自动回收

预留回收有两个触发入口：

1. 后台每 5 秒扫描内存中所有牌局。
2. `Bet`、`PrePay`、`Settlement` 和 `GetRoundFinanceState` 处理牌局时先检查到期。

到期流程：

```text
确认 reservation.Status == RESERVED
  -> 解析预留金额
  -> Redis Lua 从 agent_reserved_data 释放金额
  -> 预留状态改为 EXPIRED
  -> 牌局状态改为 EXPIRED
  -> 保存牌局快照
```

Redis 释放失败时不会修改状态，后续继续重试。正常情况下通常在到期后 5 秒内回收。

回收只释放可用奖池，不退还玩家下注。

## 11. 数据持久化

### 11.1 Redis

| 数据 | 存储方式 | 一致性方式 |
| --- | --- | --- |
| 玩家资料和余额 | `player_{userId}` HASH，余额单位为分 | 单玩家扣款使用 Lua |
| 牌局快照 | `lottery_finance_rounds` HASH | 每次状态变更 HSET |
| 有效预留 | `agent_reserved_data` ZSET | 增加和释放使用 Lua |
| 代理奖池累计值 | 四个 ZSET | 内存累计，每 30 秒覆盖写入 |
| 玩家累计统计 | 三个 ZSET | 内存累计，每 30 秒覆盖写入 |
| 配置 | `/config/*`、`/agent/*` | 启动加载，消息热更新 |
| 服务发现 | `/grpc/registry/wg-lottery/*` | TTL 续约 |

牌局快照保存：

```text
HASH lottery_finance_rounds
field = roundId
value = financeRound JSON
```

快照包含路由信息、成功下注、预留凭证、最终结算结果和状态。

### 11.2 Elasticsearch

| 索引 | 内容 | 刷新方式 |
| --- | --- | --- |
| `pp_gp_settlement` | 玩家结算注单 | 40 条或 10 秒批量写 |
| `pp_gp_flowing_water` | 下注和赔付流水 | 50 条或 10 秒批量写 |
| `pp_pool_record_log` | 奖池变化日志 | 32 条或 5 秒批量写 |

当前奖池日志在成功下注和结算后登记。`PrePay`、预留差额调整和超时释放本身没有主动登记奖池日志，因此 ES 不保证记录每一次 Q 变化。

## 12. 配置与运行时事件

Redis `message` 频道当前注册：

| 事件 | 目标行为 |
| --- | --- |
| `config` | 更新系统、币种、默认奖池、代理奖池和控制配置 |
| `addGame` | 新增游戏缓存 |
| `gameStatuChange` | 更新游戏状态 |
| `addAgent` | 目标是新增代理缓存 |
| `agentStatuChange` | 更新代理冻结状态 |
| `resetPool` | 清空内存和 Redis 中的 E/P/R/Q/打码累计值 |

`resolveRuntime` 每次 RPC 都检查代理和游戏冻结状态，并从代理奖池配置或默认奖池配置取得税率。

## 13. 并发与分布式能力

### 13.1 已实现的并发保护

1. 单玩家余额扣款使用 Redis Lua，余额不会被并发扣成负数。
2. 有效预留 Q 的检查和增加使用 Redis Lua，同一 Redis 奖池不会因并发预赔直接超额。
3. 释放预留先检查 Q 是否足够，避免重复释放扣到其他牌局。
4. 实例内奖池缓存按代理加锁。
5. 牌局状态由实例内全局 `roundLock` 串行保护。

### 13.2 当前分布式部署约束

E/P/R 来自实例内存，并每 30 秒异步写 Redis；`roundLock` 也只是实例内锁。因此：

```text
hash(agentId) -> 固定 lottery 写实例
```

同一代理不能同时由多个 lottery 实例写入。不同代理可以按代理 ID 分片到不同实例，实现横向扩展。

`GetRoundFinanceState` 没有 `agent` 字段。网关按代理路由时，需要维护 `roundId -> agentId` 映射，或者先从共享牌局快照解析代理后再转发。

Redis 中的服务注册只提供实例发现，本身没有实现按代理粘性路由、跨实例 CAS 或 fencing token。

## 14. 当前一致性和故障边界

以下步骤不是一个跨 Redis、内存、MySQL 和 ES 的原子事务：

1. `Bet` 的余额扣款、下注快照保存、E/R 更新和流水落地。
2. `PrePay` 的 Q 更新与 `RESERVED` 快照保存。
3. 结算的多玩家余额增加、P/E/R 更新、Q 释放和最终状态保存。
4. 超时回收的 Q 释放与 `EXPIRED` 快照保存。
5. 内存统计与每 30 秒 Redis 持久化。

具体风险：

1. `Bet` 在余额扣除后、成功快照保存前崩溃，重试可能再次扣款。
2. `PrePay` 在 Q 增加后、牌局快照保存前崩溃，可能产生无法由牌局定位的汇总预留。
3. 结算先增加玩家余额，后释放预留并保存最终状态；中途失败可能出现已赔付但仍未进入 `SETTLED`，再次重试存在重复赔付风险。
4. `agent_reserved_data` 只保存每个奖池的汇总 Q，没有按 `reservationId` 独立账本。
5. ES 批量写失败只记录日志，当前批次清空后没有持久重试队列。
6. 代理和玩家统计批量写失败后，`UpdateTime` 已被清零，可能丢失再次落地标记。
7. `lottery_finance_rounds` 没有 CAS、版本号、自动归档或自动清理。
8. 全部牌局共用一个 `roundLock`，锁内包含 Redis、余额和统计操作，高并发时会形成实例级串行瓶颈。

当服务无法确认资金结果时，当前业务边界是保留数据并人工审核，不自动退款。

## 15. 已知实现差异和缺口

1. `GetRoundFinanceStateResp.reservedItems` 已进入协议，但当前始终为空。
2. `reservationMode` 没有枚举或白名单校验；只有精确字符串 `LIABILITY_CAP` 具有责任上限语义。
3. `PrePay` 使用 `betDigest + outcomeHash` 快速幂等返回，不比较重复请求中的赔付明细。
4. 责任上限预留后新增下注只刷新 `betDigest`，不会自动扩大责任上限。
5. `addAgent` 事件当前注册到了 `AgentStatusChange` 处理器，消息类型与处理器断言不一致，事件触发时可能 panic，新增代理仍主要依赖 300 秒数据库重载。
6. `gameStatuChange` 当前修改 `Game.State`，RPC 冻结校验读取 `Game.IsFrozen`，热更新可能不能立即影响 RPC 校验。
7. `Server.Stop` 当前只写关闭日志，没有调用 gRPC `GracefulStop`，也没有退出前强制刷新内存统计和 ES 队列。
8. RPC 当前没有服务内认证或调用方授权校验，依赖外围网络和网关控制。
9. `VOID_REFUND` 会撤销奖池税收，但生成的结算注单仍按下注额计算 `Revenue/ExRevenue`。

## 16. 当前未实现能力

1. 主动续期预留。
2. 主动释放预留。
3. 部分退款。
4. 实际赔付超过预留后的自动追加预留。
5. 故障牌局自动续局或自动退款。
6. 跨存储分布式事务和崩溃自动补偿。
7. 多实例并发写同一代理的 CAS 或 fencing token。
8. 按 `reservationId` 保存独立预留账本。
9. 已完成牌局的自动归档和清理。
10. 完整的端到端资金故障注入和恢复测试。

## 17. 典型调用流程

### 17.1 精确结果预留

```text
Bet（首次或追加下注）
  -> 游戏封盘
  -> 生成候选结果和 outcomeHash
  -> PrePay（reservationMode 为空）
  -> 结果公开
  -> Settlement(NORMAL)
  -> GetRoundFinanceState 确认 SETTLED
```

### 17.2 单人房责任上限预留

```text
Bet（初始下注）
  -> 计算最大赔付责任
  -> PrePay(reservationMode=LIABILITY_CAP)
  -> 发牌或进入可追加下注阶段
  -> Bet（可继续追加）
  -> 可选：PrePay(LIABILITY_CAP) 调整责任上限
  -> 可选：PrePay(精确模式) 锁定最终结果
  -> Settlement(NORMAL)
```

如果不转换为精确预留，也可以直接使用责任上限预留结算，但实际赔付不能超过上限。

### 17.3 故障退款

```text
GetRoundFinanceState
  -> 人工核对玩家下注、预留和结算状态
  -> Settlement(VOID_REFUND)
  -> GetRoundFinanceState 确认 VOIDED
```

游戏进程恢复后使用新 `roundId` 开新局，不自动接续故障旧局。

## 18. 当前测试覆盖

当前自动测试主要覆盖：

1. 非负金额和分精度解析。
2. 允许负数的利润解析。
3. `symbol_level` 奖池标识构造。
4. `E - P - R - Q` 奖池公式。
5. 各 Go 模块的编译检查。

仓库另有 `tools/lottery-loadtest`，当前只对 `Bet` 做 QPS 和并发压测，不覆盖预留、结算、超时回收和故障恢复。

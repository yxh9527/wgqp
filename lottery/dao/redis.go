package dao

import (
	. "app/config"
	"app/entity"
	"context"
	"errors"
	"fmt"
	"lottery/event"
	"lottery/util"
	"micro_service/services"
	"reflect"

	"strconv"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

var redisDao *RedisDao = nil

var (
	// ErrPlayerNotCached 表示玩家完整资料尚未加载到 Redis。
	ErrPlayerNotCached = errors.New("player is not cached")
	// ErrInsufficientFunds 表示本次扣款会使玩家余额小于零。
	ErrInsufficientFunds = errors.New("insufficient player funds")
	// updateCurrencyScript 将存在性检查、余额非负校验、增减款和脏数据标记合并为一次原子操作。
	updateCurrencyScript = redis.NewScript(`
local playerKey = KEYS[1]
if redis.call("HEXISTS", playerKey, "id") == 0 then
    return {0, 0}
end
local current = tonumber(redis.call("HGET", playerKey, "currency") or "0")
local updated = current + tonumber(ARGV[1])
if updated < 0 then
    return {1, current}
end
redis.call("HINCRBY", playerKey, "currency", ARGV[1])
redis.call("HINCRBY", playerKey, "exp", ARGV[2])
redis.call("EXPIRE", playerKey, ARGV[3])
redis.call("SADD", KEYS[2], ARGV[4])
return {2, updated}
`)
	// refundWithCancelMarkerScript：先入账再写 marker。
	// marker 仅表示钱包已实际入账；若 marker 已存在则跳过入账（幂等重试）。
	// 返回：{0,0} 未缓存；{1,bal} 余额不足；{2,bal} 本次新入账；{3,bal} 已入账过。
	refundWithCancelMarkerScript = redis.NewScript(`
local playerKey = KEYS[1]
local dirtyKey = KEYS[2]
local markerKey = KEYS[3]
local delta = tonumber(ARGV[1])
local exp = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local playerId = ARGV[4]
local markerValue = ARGV[5]
local markerTTL = tonumber(ARGV[6])
if redis.call("EXISTS", markerKey) == 1 then
    local current = tonumber(redis.call("HGET", playerKey, "currency") or "0")
    return {3, current}
end
if redis.call("HEXISTS", playerKey, "id") == 0 then
    return {0, 0}
end
local current = tonumber(redis.call("HGET", playerKey, "currency") or "0")
local updated = current + delta
if updated < 0 then
    return {1, current}
end
redis.call("HINCRBY", playerKey, "currency", delta)
if exp ~= 0 then
    redis.call("HINCRBY", playerKey, "exp", exp)
end
redis.call("EXPIRE", playerKey, ttl)
redis.call("SADD", dirtyKey, playerId)
redis.call("SET", markerKey, markerValue, "EX", markerTTL)
return {2, updated}
`)
	// tryReservePoolScript 原子检查可用奖池并增加预留，避免并发预赔透支。
	tryReservePoolScript = redis.NewScript(`
local current = tonumber(redis.call("ZSCORE", KEYS[1], ARGV[1]) or "0")
local basePool = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
if basePool - current < requested then
    return 0
end
redis.call("ZINCRBY", KEYS[1], requested, ARGV[1])
return 1
`)
	// releasePoolReservationScript 校验预留余额后再释放，防止重复释放影响其他牌局。
	releasePoolReservationScript = redis.NewScript(`
local current = tonumber(redis.call("ZSCORE", KEYS[1], ARGV[1]) or "0")
local released = tonumber(ARGV[2])
if current + 0.0000001 < released then
    return 0
end
local updated = current - released
if updated <= 0.0000001 then
    redis.call("ZREM", KEYS[1], ARGV[1])
else
    redis.call("ZADD", KEYS[1], updated, ARGV[1])
end
return 1
`)
	// reserveWithLedgerScript：汇总 Q + 按 reservationId 建 ledger。
	// KEYS[1]=agent_reserved_data KEYS[2]=lottery:reservation_ledger
	// ARGV: poolMember, basePool, amount, reservationId, ledgerValue(ACTIVE|...)
	// 返回：0 不足；1 新建成功；2 已是 ACTIVE（幂等，不加 Q）；3 已 RELEASED 拒绝复用。
	reserveWithLedgerScript = redis.NewScript(`
local reservationId = ARGV[4]
local existing = redis.call("HGET", KEYS[2], reservationId)
if existing then
  local status = string.match(existing, "^([^|]+)")
  if status == "ACTIVE" then
    return 2
  end
  return 3
end
local current = tonumber(redis.call("ZSCORE", KEYS[1], ARGV[1]) or "0")
local basePool = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
if basePool - current < requested then
  return 0
end
redis.call("ZINCRBY", KEYS[1], requested, ARGV[1])
redis.call("HSET", KEYS[2], reservationId, ARGV[5])
return 1
`)
	// releaseWithLedgerScript：校验 reservationId ledger 后释放 Q。
	// ARGV: poolMember, amount, reservationId, releaseId, releasedLedgerValue, allowMigrate(0/1), migrateLedgerBefore
	// 返回：0 失败；1 本次新释放；2 已释放（幂等，不改 Q）。
	releaseWithLedgerScript = redis.NewScript(`
local reservationId = ARGV[3]
local releaseId = ARGV[4]
local existing = redis.call("HGET", KEYS[2], reservationId)
if not existing then
  if tonumber(ARGV[6]) ~= 1 then
    return 0
  end
  -- 升级兼容：无 ledger 的旧预留，允许一次性补建并释放。
  existing = ARGV[7]
  redis.call("HSET", KEYS[2], reservationId, existing)
end
local status = string.match(existing, "^([^|]+)")
if status == "RELEASED" then
  return 2
end
if status ~= "ACTIVE" then
  return 0
end
local current = tonumber(redis.call("ZSCORE", KEYS[1], ARGV[1]) or "0")
local released = tonumber(ARGV[2])
if current + 0.0000001 < released then
  return 0
end
local updated = current - released
if updated <= 0.0000001 then
  redis.call("ZREM", KEYS[1], ARGV[1])
else
  redis.call("ZADD", KEYS[1], updated, ARGV[1])
end
redis.call("HSET", KEYS[2], reservationId, ARGV[5])
return 1
`)
	// adjustReservationLedgerScript：LIABILITY_CAP 差额调整，保持 ACTIVE。
	// ARGV: poolMember, basePool, reservationId, oldAmount, newAmount, activeLedgerValue
	// 返回：0 失败；1 成功；2 ledger 缺失且不允许（由 Go 侧决定是否 migrate 后重试）。
	adjustReservationLedgerScript = redis.NewScript(`
local reservationId = ARGV[3]
local existing = redis.call("HGET", KEYS[2], reservationId)
if not existing then
  return 2
end
local status = string.match(existing, "^([^|]+)")
if status ~= "ACTIVE" then
  return 0
end
local oldAmount = tonumber(ARGV[4])
local newAmount = tonumber(ARGV[5])
local delta = newAmount - oldAmount
local current = tonumber(redis.call("ZSCORE", KEYS[1], ARGV[1]) or "0")
if delta > 0 then
  local basePool = tonumber(ARGV[2])
  if basePool - current < delta then
    return 0
  end
  redis.call("ZINCRBY", KEYS[1], delta, ARGV[1])
elseif delta < 0 then
  local released = -delta
  if current + 0.0000001 < released then
    return 0
  end
  local updated = current - released
  if updated <= 0.0000001 then
    redis.call("ZREM", KEYS[1], ARGV[1])
  else
    redis.call("ZADD", KEYS[1], updated, ARGV[1])
  end
end
redis.call("HSET", KEYS[2], reservationId, ARGV[6])
return 1
`)
)

const (
	reservationLedgerRedisKey = "lottery:reservation_ledger"
	financeReconcileSetKey    = "lottery:finance_reconcile"
	settleWalletMarkerPrefix  = "lottery:settle_wallet:"
	SettleWalletMarkerTTLSec  = int32(7 * 24 * 3600)
)

// ReservationLedger 状态。
const (
	ReservationLedgerActive   = "ACTIVE"
	ReservationLedgerReleased = "RELEASED"
)

// EncodeReservationLedger 使用 Lua 友好的管道分隔格式。
// status|amount|releaseId|roundId|agent|poolSymbol|reason|updatedAt
func EncodeReservationLedger(
	status, amount, releaseID, roundID string,
	agent int64,
	poolSymbol, reason string,
	updatedAt int64,
) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s|%d",
		status, amount, releaseID, roundID, agent, poolSymbol, reason, updatedAt)
}

// ParseReservationLedgerStatus 解析 ledger value 的 status 字段。
func ParseReservationLedgerStatus(raw string) string {
	if raw == "" {
		return ""
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] == '|' {
			return raw[:i]
		}
	}
	return raw
}

// SettleWalletMarkerKey 结算入账幂等键。
func SettleWalletMarkerKey(settlementID string, userID uint32) string {
	return fmt.Sprintf("%s%s:%d", settleWalletMarkerPrefix, settlementID, userID)
}

type RedisDao struct {
	cli redis.UniversalClient
}

func RedisIns() *RedisDao {
	return redisDao
}

// SetRedisDaoForTest 仅测试注入 Redis 客户端。
func SetRedisDaoForTest(rd *RedisDao) {
	redisDao = rd
}

// NewRedisDaoWithClient 仅测试：用已有客户端构造 RedisDao。
func NewRedisDaoWithClient(cli redis.UniversalClient) *RedisDao {
	return &RedisDao{cli: cli}
}

func NewRedisDao(hosts []string, user, pwd string) {
	if redisDao == nil {
		cli := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:                 hosts,
			Password:              pwd,
			Username:              user,
			DB:                    0,
			PoolSize:              100,
			MinIdleConns:          30,
			MaxIdleConns:          0,
			ConnMaxIdleTime:       60 * time.Second,
			ConnMaxLifetime:       5 * time.Minute,
			ReadBufferSize:        32 * 1024,
			WriteBufferSize:       32 * 1024,
			PoolTimeout:           5 * time.Second,
			ReadTimeout:           10 * time.Second,
			WriteTimeout:          10 * time.Second,
			ContextTimeoutEnabled: true,
		})
		redisDao = &RedisDao{cli: cli}
		redisDao.Subscribe("message", func() *event.EventMgr {
			e := &event.EventMgr{
				Events: make(map[string]*event.Event),
			}
			e.Register("config", reflect.TypeOf(entity.ConfigMsg{}), ConfigHandler)
			e.Register("addGame", reflect.TypeOf(entity.AddGame{}), AddGame)
			e.Register("gameStatuChange", reflect.TypeOf(entity.GameStatuChange{}), GameStatusChange)
			e.Register("addAgent", reflect.TypeOf(entity.AddAgent{}), AgentStatusChange)
			e.Register("agentStatuChange", reflect.TypeOf(entity.AgentStatuChange{}), AgentStatusChange)
			e.Register("resetPool", reflect.TypeOf(entity.ResetPool{}), ResetPool)
			return e
		})
	}
}

// 配置同步
func ConfigHandler(data interface{}) {
	msg := data.(*entity.ConfigMsg)
	util.ParseConfig(msg.Key, msg.Data)
}

func AddGame(data interface{}) {
	add := data.(*entity.AddGame)
	zap.L().Debug("添加游戏配置", zap.Any("add", add))
	GamesManagerIns().Add(add.Game)
}

func GameStatusChange(data interface{}) {
	change := data.(*entity.GameStatuChange)
	zap.L().Debug("修改游戏状态", zap.Any("change", change))
	if g := GamesManagerIns().Get(change.Symbol); g != nil {
		g.State = int16(change.Status)
	}
}

func AddAgent(data interface{}) {
	add := data.(*entity.AddAgent)
	zap.L().Debug("添加代理配置", zap.Any("add", add))
	AgentManagerIns().Add(add.Agent)
}

func AgentStatusChange(data interface{}) {
	change := data.(*entity.AgentStatuChange)
	zap.L().Debug("修改代理状态", zap.Any("change", change))
	AgentManagerIns().ChangeStatus(change.Id, change.Status)
}

func (rd *RedisDao) GetPlayer(playerId, factory uint32) (*services.HumanPlayer, error) {
	res, err := rd.cli.HGetAll(context.Background(), "player_"+strconv.FormatUint(uint64(playerId), 10)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	var p services.HumanPlayer
	zap.L().Info("Redis:GetPlayer", zap.Uint32("player_id", playerId), zap.Any("player", res))
	for key, value := range res {
		switch key {
		case "id":
			id, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, err
			}
			p.Id = uint32(id)
		case "nickname":
			p.Nickname = value
		case "currency":
			strCurrency, err := decimal.NewFromString(value)
			if err != nil {
				return nil, err
			}
			// redis中存储的单位为分，这里需要转换一下
			p.GameCurrency = strCurrency.Div(decimal.New(1, 2)).StringFixed(2)
		case "avatar":
			p.Avatar = value
		case "gender":
			gender, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, err
			}
			p.Gender = uint32(gender)
		case "exp":
		case "agent_id":
			agentId, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, err
			}
			p.AgentId = uint32(agentId)
		case "login_ip":
			p.LoginIP = value
		case "login_time":
			loginTimestamp, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
			p.LoginTimeStamp = loginTimestamp
		case "currency_limit":
			p.CurrencyLimit = value
		case "account":
			p.Account = value
		case "currency_type":
			p.CurrencyType = value
		case "website_id":
			websiteId, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, err
			}
			p.WebSiteId = uint32(websiteId)
		case "all_times":
			allTimes, _ := strconv.Atoi(value)
			p.AllTimes = int32(allTimes)
		case "isTourist":
			isTourist, _ := strconv.Atoi(value)
			p.IsTourist = int32(isTourist)
		}
	}
	return &p, nil
}

func (rd *RedisDao) SetPlayer(p *services.HumanPlayer) error {
	pID := "player_" + strconv.FormatUint(uint64(p.Id), 10)
	pipe := rd.cli.Pipeline()
	pipe.HSet(context.Background(), pID, map[string]interface{}{
		"id":             p.Id,
		"nickname":       p.Nickname,
		"currency":       decimal.RequireFromString(p.GameCurrency).Mul(decimal.New(1, 2)).IntPart(), // 方便incr，转换为分
		"avatar":         p.Avatar,
		"gender":         p.Gender,
		"exp":            p.Exp,
		"agent_id":       p.AgentId,
		"login_ip":       p.LoginIP,
		"login_time":     p.LoginTimeStamp,
		"currency_limit": p.CurrencyLimit,
		"website_id":     p.WebSiteId,
		"account":        p.Account,
		"currency_type":  p.CurrencyType,
		"all_times":      p.AllTimes,
		"isTourist":      p.IsTourist,
	})
	pipe.Expire(context.Background(), pID, time.Minute*20)
	pipe.SAdd(context.Background(), "dirty_list", p.Id)
	_, err := pipe.Exec(context.Background())
	return err
}

// UpdatePlayerCurrency 以“分”为单位原子增减玩家余额，并保证并发扣款后余额不会小于零。
func (rd *RedisDao) UpdatePlayerCurrency(playerId uint32, currencyDelta int64, exp, factory uint32, source int) (newCurrency int64, err error) {
	pID := "player_" + strconv.FormatUint(uint64(playerId), 10)
	result, err := updateCurrencyScript.Run(
		context.Background(),
		rd.cli,
		[]string{pID, "dirty_list_imp"},
		currencyDelta,
		exp,
		int64((20*time.Minute)/time.Second),
		playerId,
	).Slice()
	if err != nil {
		return 0, err
	}
	if len(result) != 2 {
		return 0, fmt.Errorf("unexpected currency script result")
	}
	status, ok := result[0].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected currency script status")
	}
	value, ok := result[1].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected currency script balance")
	}
	switch status {
	case 0:
		return 0, ErrPlayerNotCached
	case 1:
		return value, ErrInsufficientFunds
	case 2:
		return value, nil
	default:
		return 0, fmt.Errorf("unknown currency script status: %d", status)
	}
}

// CancelWalletRefundResult 描述 CancelBet 钱包原子退款结果。
type CancelWalletRefundResult struct {
	// AlreadyDone 为 true 表示 marker 已存在，本次未再次入账。
	AlreadyDone bool
	NewCurrency int64
}

// RefundPlayerCurrencyWithCancelMarker 原子完成：余额入账 + 写入 cancel wallet marker。
// marker 只在入账成功后写入；重试时若 marker 已存在则不再入账。
func (rd *RedisDao) RefundPlayerCurrencyWithCancelMarker(
	playerId uint32,
	currencyDelta int64,
	markerKey string,
	markerValue string,
	markerTTLSeconds int32,
) (CancelWalletRefundResult, error) {
	pID := "player_" + strconv.FormatUint(uint64(playerId), 10)
	result, err := refundWithCancelMarkerScript.Run(
		context.Background(),
		rd.cli,
		[]string{pID, "dirty_list_imp", markerKey},
		currencyDelta,
		0,
		int64((20*time.Minute)/time.Second),
		playerId,
		markerValue,
		int64(markerTTLSeconds),
	).Slice()
	if err != nil {
		return CancelWalletRefundResult{}, err
	}
	if len(result) != 2 {
		return CancelWalletRefundResult{}, fmt.Errorf("unexpected cancel refund script result")
	}
	status, ok := result[0].(int64)
	if !ok {
		return CancelWalletRefundResult{}, fmt.Errorf("unexpected cancel refund script status")
	}
	value, ok := result[1].(int64)
	if !ok {
		return CancelWalletRefundResult{}, fmt.Errorf("unexpected cancel refund script balance")
	}
	switch status {
	case 0:
		return CancelWalletRefundResult{}, ErrPlayerNotCached
	case 1:
		return CancelWalletRefundResult{NewCurrency: value}, ErrInsufficientFunds
	case 2:
		return CancelWalletRefundResult{AlreadyDone: false, NewCurrency: value}, nil
	case 3:
		return CancelWalletRefundResult{AlreadyDone: true, NewCurrency: value}, nil
	default:
		return CancelWalletRefundResult{}, fmt.Errorf("unknown cancel refund script status: %d", status)
	}
}

func (rd *RedisDao) GetPlayerCurrency(playerId uint32) (newCurrency int64, err error) {
	pID := "player_" + strconv.FormatUint(uint64(playerId), 10)
	pipe := rd.cli.Pipeline()
	pipe.HExists(context.Background(), pID, "id").Result() // 完整的player信息是否存在
	pipe.Expire(context.Background(), pID, time.Minute*20)
	pipe.HGet(context.Background(), pID, "currency")
	result, e := pipe.Exec(context.Background())
	if e != nil {
		return 0, e
	}
	sc := result[2].(*redis.StringCmd)
	nc, err := sc.Result()
	if err != nil {
		return 0, err
	}
	newCurrency, err = strconv.ParseInt(nc, 10, 64)
	if err != nil {
		return 0, err
	}
	return newCurrency, nil
}

// 批量获取玩家金币信息，只有再玩家登录后才能使用该接口 且 返回金币是未换算的值
func (rd *RedisDao) BatchGetPlayerCurrencys(ids []uint32) (map[uint32]int64, error) {
	currencys := make(map[uint32]int64)
	pipe := rd.cli.Pipeline()
	for _, id := range ids {
		pID := fmt.Sprintf("player_%d", id)
		pipe.HGet(context.Background(), pID, "currency")
	}
	result, e := pipe.Exec(context.Background())
	if e != nil {
		return nil, e
	}
	for i, id := range ids {
		sc := result[i].(*redis.StringCmd)
		nc, err := sc.Result()
		if err != nil {
			continue
		}
		newCurrency, _ := strconv.ParseInt(nc, 10, 64)
		currencys[id] = newCurrency
	}
	return currencys, nil
}

// 批量修改玩家金币信息，只有再玩家登录后才能使用该接口 且 返回金币是未换算的值
// BatchUpdatePlayerCurrencys 批量发放已经完整校验过的结算金额，并返回每个玩家的新余额。
func (rd *RedisDao) BatchUpdatePlayerCurrencys(deltas map[uint32]int64) (map[uint32]int64, error) {
	currencys := make(map[uint32]int64)
	pipe := rd.cli.Pipeline()
	ids := make([]uint32, 0, len(deltas))
	for id, delta := range deltas {
		ids = append(ids, id)
		pID := fmt.Sprintf("player_%d", id)
		pipe.HIncrBy(context.Background(), pID, "currency", delta)
	}
	result, e := pipe.Exec(context.Background())
	if e != nil {
		return nil, e
	}
	for i, id := range ids {
		sc := result[i].(*redis.IntCmd)
		nc, err := sc.Result()
		if err != nil {
			continue
		}
		currencys[id] = nc
	}

	//触发数据落地
	pipe = rd.cli.Pipeline()
	for id := range deltas {
		pipe.SAdd(context.Background(), "dirty_list_imp", id)
	}
	pipe.Exec(context.Background())

	return currencys, nil
}

func (rd *RedisDao) GetUserStatData(id uint32) (decimal.Decimal, decimal.Decimal, decimal.Decimal, string, int32) {
	itemKey := fmt.Sprintf("%d", id)
	piple := rd.cli.Pipeline()
	piple.ZIncrBy(context.Background(), "userTotalEffBet", 0, itemKey)
	piple.ZIncrBy(context.Background(), "userTotalProfLoss", 0, itemKey)
	piple.ZIncrBy(context.Background(), "userBetCount", 0, itemKey)
	piple.HGet(context.Background(), fmt.Sprintf("player_%d", id), "account")
	piple.HGet(context.Background(), fmt.Sprintf("player_%d", id), "isTourist")
	result, err := piple.Exec(context.Background())
	if err == nil {
		eff, effErr := result[0].(*redis.FloatCmd).Result()
		if effErr != nil {
			zap.L().Error("获取有效投注失败", zap.Error(effErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
		}
		pro, proErr := result[1].(*redis.FloatCmd).Result()
		if proErr != nil {
			zap.L().Error("获取盈亏信息失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
		}
		count, cError := result[2].(*redis.FloatCmd).Result()
		if cError != nil {
			zap.L().Error("获取下注次数失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
		}
		account, accError := result[3].(*redis.StringCmd).Result()
		if accError != nil {
			zap.L().Error("获取下注次数失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
		}
		isTouristStr, touristErr := result[4].(*redis.StringCmd).Result()
		if touristErr != nil {
			zap.L().Error("获取游客标志失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
		}
		isTourist, _ := strconv.Atoi(isTouristStr)
		return decimal.NewFromFloat(eff), decimal.NewFromFloat(pro), decimal.NewFromFloat(count), account, int32(isTourist)
	}
	return decimal.Zero, decimal.Zero, decimal.Zero, "", 0
}

func (rd *RedisDao) GetGameStatData(id int64, symbol string) (decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal, bool) {
	itemKey := fmt.Sprintf("%d_%s", id, symbol)
	piple := rd.cli.Pipeline()
	piple.ZIncrBy(context.Background(), "agent_effect_data", 0, itemKey)
	piple.ZIncrBy(context.Background(), "agent_chips_data", 0, itemKey)
	piple.ZIncrBy(context.Background(), "agent_profitLoss_data", 0, itemKey)
	piple.ZIncrBy(context.Background(), "agent_revenue_data", 0, itemKey)
	result, err := piple.Exec(context.Background())
	if err == nil {
		eff, effErr := result[0].(*redis.FloatCmd).Result()
		if effErr != nil {
			zap.L().Error("获取有效投注失败", zap.Error(effErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, false
		}
		chips, chipsErr := result[1].(*redis.FloatCmd).Result()
		if chipsErr != nil {
			zap.L().Error("获取盈亏信息失败", zap.Error(chipsErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, false
		}
		pro, proErr := result[2].(*redis.FloatCmd).Result()
		if proErr != nil {
			zap.L().Error("获取盈亏信息失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, false
		}
		revenue, rError := result[3].(*redis.FloatCmd).Result()
		if rError != nil {
			zap.L().Error("获取盈亏信息失败", zap.Error(proErr))
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, false
		}
		return decimal.NewFromFloat(eff), decimal.NewFromFloat(chips), decimal.NewFromFloat(pro), decimal.NewFromFloat(revenue), true
	}
	return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, false
}

// GetAgentReserved 读取代理在指定奖池上的有效预留总额，金额单位为 CNY。
func (rd *RedisDao) GetAgentReserved(agentID int64, symbol string) decimal.Decimal {
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	value, err := rd.cli.ZScore(context.Background(), "agent_reserved_data", key).Result()
	if err == redis.Nil {
		return decimal.Zero
	}
	if err != nil {
		zap.L().Error("读取奖池有效预留失败", zap.Int64("agentId", agentID), zap.String("symbol", symbol), zap.Error(err))
		return decimal.Zero
	}
	return decimal.NewFromFloat(value)
}

// TryReserveAgentPool 原子判断基础奖池扣除现有预留后是否足够，并在足够时增加预留。
func (rd *RedisDao) TryReserveAgentPool(agentID int64, symbol string, basePool, amount decimal.Decimal) (bool, error) {
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	result, err := tryReservePoolScript.Run(
		context.Background(),
		rd.cli,
		[]string{"agent_reserved_data"},
		key,
		basePool.String(),
		amount.String(),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// ReleaseAgentReserved 原子释放指定金额；当前预留不足时拒绝操作。
// Deprecated: 新路径请用 ReleaseAgentReservedWithLedger，避免跨牌局误释放。
func (rd *RedisDao) ReleaseAgentReserved(agentID int64, symbol string, amount decimal.Decimal) (bool, error) {
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	result, err := releasePoolReservationScript.Run(
		context.Background(),
		rd.cli,
		[]string{"agent_reserved_data"},
		key,
		amount.String(),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// ReserveWithLedgerResult 描述带 ledger 的预留结果。
type ReserveWithLedgerResult struct {
	// Ok 表示可用（新建或幂等命中 ACTIVE）。
	Ok bool
	// AlreadyActive 为 true 时未再次增加 Q。
	AlreadyActive bool
	// RejectedReleased 表示 reservationId 已 RELEASED，禁止复用。
	RejectedReleased bool
}

// TryReserveAgentPoolWithLedger 在增加 Q 的同时写入 reservationId ledger。
func (rd *RedisDao) TryReserveAgentPoolWithLedger(
	agentID int64,
	symbol string,
	basePool, amount decimal.Decimal,
	reservationID, roundID string,
) (ReserveWithLedgerResult, error) {
	if reservationID == "" {
		return ReserveWithLedgerResult{}, fmt.Errorf("reservationId required")
	}
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	ledger := EncodeReservationLedger(
		ReservationLedgerActive,
		amount.String(),
		"",
		roundID,
		agentID,
		symbol,
		"",
		time.Now().Unix(),
	)
	code, err := reserveWithLedgerScript.Run(
		context.Background(),
		rd.cli,
		[]string{"agent_reserved_data", reservationLedgerRedisKey},
		key,
		basePool.String(),
		amount.String(),
		reservationID,
		ledger,
	).Int64()
	if err != nil {
		return ReserveWithLedgerResult{}, err
	}
	switch code {
	case 0:
		return ReserveWithLedgerResult{Ok: false}, nil
	case 1:
		return ReserveWithLedgerResult{Ok: true}, nil
	case 2:
		return ReserveWithLedgerResult{Ok: true, AlreadyActive: true}, nil
	case 3:
		return ReserveWithLedgerResult{RejectedReleased: true}, nil
	default:
		return ReserveWithLedgerResult{}, fmt.Errorf("unknown reserveWithLedger status: %d", code)
	}
}

// ReleaseWithLedgerResult 描述带 ledger 的释放结果。
type ReleaseWithLedgerResult struct {
	Ok              bool
	AlreadyReleased bool
}

// ReleaseAgentReservedWithLedger 按 reservationId 幂等释放；已 RELEASED 不再扣 Q。
// allowMigrate：ledger 缺失时补建 ACTIVE 再释放（兼容升级前旧预留）。
func (rd *RedisDao) ReleaseAgentReservedWithLedger(
	agentID int64,
	symbol string,
	amount decimal.Decimal,
	reservationID, roundID, releaseID, reason string,
	allowMigrate bool,
) (ReleaseWithLedgerResult, error) {
	if reservationID == "" {
		return ReleaseWithLedgerResult{}, fmt.Errorf("reservationId required")
	}
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	now := time.Now().Unix()
	releasedLedger := EncodeReservationLedger(
		ReservationLedgerReleased,
		amount.String(),
		releaseID,
		roundID,
		agentID,
		symbol,
		reason,
		now,
	)
	migrateLedger := EncodeReservationLedger(
		ReservationLedgerActive,
		amount.String(),
		"",
		roundID,
		agentID,
		symbol,
		"",
		now,
	)
	migrateFlag := 0
	if allowMigrate {
		migrateFlag = 1
	}
	code, err := releaseWithLedgerScript.Run(
		context.Background(),
		rd.cli,
		[]string{"agent_reserved_data", reservationLedgerRedisKey},
		key,
		amount.String(),
		reservationID,
		releaseID,
		releasedLedger,
		migrateFlag,
		migrateLedger,
	).Int64()
	if err != nil {
		return ReleaseWithLedgerResult{}, err
	}
	switch code {
	case 0:
		return ReleaseWithLedgerResult{Ok: false}, nil
	case 1:
		return ReleaseWithLedgerResult{Ok: true}, nil
	case 2:
		return ReleaseWithLedgerResult{Ok: true, AlreadyReleased: true}, nil
	default:
		return ReleaseWithLedgerResult{}, fmt.Errorf("unknown releaseWithLedger status: %d", code)
	}
}

// AdjustAgentReservedWithLedger 调整同一 reservationId 的占用额，保持 ACTIVE。
func (rd *RedisDao) AdjustAgentReservedWithLedger(
	agentID int64,
	symbol string,
	basePool, oldAmount, newAmount decimal.Decimal,
	reservationID, roundID string,
) (bool, error) {
	if reservationID == "" {
		return false, fmt.Errorf("reservationId required")
	}
	key := fmt.Sprintf("%d_%s", agentID, symbol)
	ledger := EncodeReservationLedger(
		ReservationLedgerActive,
		newAmount.String(),
		"",
		roundID,
		agentID,
		symbol,
		"",
		time.Now().Unix(),
	)
	code, err := adjustReservationLedgerScript.Run(
		context.Background(),
		rd.cli,
		[]string{"agent_reserved_data", reservationLedgerRedisKey},
		key,
		basePool.String(),
		reservationID,
		oldAmount.String(),
		newAmount.String(),
		ledger,
	).Int64()
	if err != nil {
		return false, err
	}
	switch code {
	case 1:
		return true, nil
	case 0, 2:
		return false, nil
	default:
		return false, fmt.Errorf("unknown adjustReservationLedger status: %d", code)
	}
}

// GetReservationLedgerRaw 读取 ledger 原文（测试/对账）。
func (rd *RedisDao) GetReservationLedgerRaw(reservationID string) (string, error) {
	val, err := rd.cli.HGet(context.Background(), reservationLedgerRedisKey, reservationID).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// SeedReservationLedgerActive 仅写入 ACTIVE ledger，不改 Q（升级兼容：Q 已占用但无凭证）。
// 若 field 已存在则不覆盖，返回 false。
func (rd *RedisDao) SeedReservationLedgerActive(
	agentID int64,
	symbol string,
	amount decimal.Decimal,
	reservationID, roundID string,
) error {
	if reservationID == "" {
		return fmt.Errorf("reservationId required")
	}
	ledger := EncodeReservationLedger(
		ReservationLedgerActive,
		amount.String(),
		"",
		roundID,
		agentID,
		symbol,
		"",
		time.Now().Unix(),
	)
	ok, err := rd.cli.HSetNX(context.Background(), reservationLedgerRedisKey, reservationID, ledger).Result()
	if err != nil {
		return err
	}
	if !ok {
		// 已存在则视为成功（可能并发补建）。
		return nil
	}
	return nil
}

// AddFinanceReconcileRound 将需要补偿落盘的 roundId 记入集合。
func (rd *RedisDao) AddFinanceReconcileRound(roundID string) error {
	if roundID == "" {
		return nil
	}
	return rd.cli.SAdd(context.Background(), financeReconcileSetKey, roundID).Err()
}

// PopFinanceReconcileRounds 取出一批待补偿 roundId（非事务，允许重复）。
func (rd *RedisDao) PopFinanceReconcileRounds(limit int64) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}
	ids, err := rd.cli.SRandMemberN(context.Background(), financeReconcileSetKey, limit).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	members := make([]interface{}, len(ids))
	for i, id := range ids {
		members[i] = id
	}
	_ = rd.cli.SRem(context.Background(), financeReconcileSetKey, members...).Err()
	return ids, nil
}

// CreditPlayerCurrencyWithSettleMarker 结算入账幂等：同一 settlementId+userId 只加一次币。
func (rd *RedisDao) CreditPlayerCurrencyWithSettleMarker(
	playerID uint32,
	currencyDelta int64,
	settlementID string,
) (CancelWalletRefundResult, error) {
	return rd.RefundPlayerCurrencyWithCancelMarker(
		playerID,
		currencyDelta,
		SettleWalletMarkerKey(settlementID, playerID),
		settlementID,
		SettleWalletMarkerTTLSec,
	)
}

func (rd *RedisDao) HGet(key, attrKey string) (string, error) {
	return rd.cli.HGet(context.Background(), key, attrKey).Result()
}

// HGetAll 读取统一资金服务持久化的全部牌局快照。
func (rd *RedisDao) HGetAll(key string) (map[string]string, error) {
	return rd.cli.HGetAll(context.Background(), key).Result()
}

func (rd *RedisDao) Set(key, value string, timeout int32) error {
	var to time.Duration
	if timeout > 0 {
		to = time.Duration(timeout) * time.Second
	} else {
		to = -1
	}

	return rd.cli.Set(context.Background(), key, value, to).Err()
}

// SetNX 仅当 key 不存在时写入；用于 CancelBet 钱包退款幂等标记。
func (rd *RedisDao) SetNX(key, value string, timeout int32) (bool, error) {
	var to time.Duration
	if timeout > 0 {
		to = time.Duration(timeout) * time.Second
	} else {
		to = 0
	}
	return rd.cli.SetNX(context.Background(), key, value, to).Result()
}

func (rd *RedisDao) Del(key string) error {
	return rd.cli.Del(context.Background(), key).Err()
}

func (rd *RedisDao) Get(key string) (string, error) {
	return rd.cli.Get(context.Background(), key).Result()
}

func (rd *RedisDao) HSet(key, value, attrKey string) error {
	return rd.cli.HSet(context.Background(), key, attrKey, value).Err()
}

func (rd *RedisDao) HDel(key, attrKey string) error {
	return rd.cli.HDel(context.Background(), key, attrKey).Err()
}

func (rd *RedisDao) SetKeyTimeOut(key string, timeout int32) (bool, error) {
	if timeout < 0 {
		return rd.cli.Persist(context.Background(), key).Result()
	}
	return rd.cli.Expire(context.Background(), key, time.Duration(timeout)*time.Second).Result()
}

func (rd *RedisDao) Subscribe(channel string, registryInfo func() *event.EventMgr) {
	//watch
	sub := rd.cli.Subscribe(context.Background(), channel)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zap.L().Error("panic", zap.Any("err", err))
			}
		}()
		//注册处理方法
		event := registryInfo()
		//关闭订阅
		defer sub.Close()
		for msg := range sub.Channel() {
			pack := &entity.Msg{}
			if err := jsoniter.UnmarshalFromString(msg.Payload, pack); err != nil {
				zap.L().Error("收到无法解析的推送", zap.Any("msg", msg))
			} else {
				event.OnEvent(pack)
			}
		}
	}()
}

func LoadConfigs(client *RedisDao, prefix string) {
	//初始化加载所有配置
	iter := client.cli.Scan(context.Background(), 0, prefix, 0).Iterator()
	keys := make([]string, 0, 256)
	for iter.Next(context.Background()) {
		k := iter.Val()
		keys = append(keys, k)
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	tmpKey := make([]string, 0, 256)
	//加载数据
	pipe := client.cli.Pipeline()
	for _, k := range keys {
		pipe.Get(context.Background(), k)
		tmpKey = append(tmpKey, k)
		if len(tmpKey) >= 256 {
			result, err := pipe.Exec(context.Background())
			if err != nil {
				zap.L().Fatal("加载配置数据失败", zap.Any("err", err))
			}
			for p, r := range result {
				cmd := r.(*redis.StringCmd)
				util.ParseConfig(tmpKey[p], cmd.Val())
			}
			tmpKey = make([]string, 0, 256)
			pipe = client.cli.Pipeline()
		}
	}
	if len(tmpKey) > 0 {
		//加载数据
		result, err := pipe.Exec(context.Background())
		if err != nil {
			zap.L().Fatal("加载配置数据失败", zap.Any("err", err))
		}
		for p, r := range result {
			cmd := r.(*redis.StringCmd)
			util.ParseConfig(tmpKey[p], cmd.Val())
		}
	}
}

// 初始化配置信息
func ConfigsInit() {
	if CfgIns == nil {
		CfgIns = &Configs{
			Lock: &sync.RWMutex{},
			Pool: &PoolMgr{
				Default: make(map[string]*Pool),
				Agent:   make(map[string]*Pool),
			},
			Award: &AwardMgr{
				Data: make(map[string]*AwardConfig),
			},
			System: &SystemConfig{},
			Currency: &CurrencyMgr{
				Data: make(map[string]decimal.Decimal),
			},
		}
		//加载默认配置
		LoadConfigs(RedisIns(), "/config/*")
		//加载代理配置
		LoadConfigs(RedisIns(), "/agent/*")
	}
}

func LoadConfig() *SystemConfig {
	var sysConfig *SystemConfig = &SystemConfig{}
	value, err := RedisIns().Get("/config/system")
	if err != nil {
		zap.L().Fatal("获取系统配置失败", zap.Any("err", err))
	}
	err = jsoniter.UnmarshalFromString(value, sysConfig)
	if err != nil {
		zap.L().Fatal("解析系统配置失败", zap.Any("err", err))
	}
	return sysConfig
}

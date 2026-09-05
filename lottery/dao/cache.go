package dao

import (
	"app/config"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

var gameCache *GameCacheMgr

// User 保存玩家级累计统计；所有金额都使用玩家原币种。
type User struct {
	TotalEffectBet decimal.Decimal
	TotalProfLoss  decimal.Decimal
	Count          decimal.Decimal
	UpdateTime     int64
	UserId         uint32
	AgentId        int64
	Account        string
	IsTourist      int32
}

// Game 保存单个 agent + symbol + level 奖池的 CNY 累计值。
type Game struct {
	TotalEffectBet decimal.Decimal // 代理累计有效下注。
	TotalChips     decimal.Decimal
	TotalProfLoss  decimal.Decimal // 代理累计总赔付，即每局玩家实际 payout 之和。
	TotalRevenue   decimal.Decimal // 代理累计税收。
	UpdateTime     int64
	AgentId        int64
	Symbol         string
}

// AgentData 隔离不同代理下的玩家统计和分等级游戏奖池。
type AgentData struct {
	lock      *sync.RWMutex
	Id        int64
	WebId     int64
	gameCache map[string]*Game
	userCache map[uint32]*User
}

func (ad *AgentData) GetUser(userID uint32) *User {
	user := ad.userCache[userID]
	if user != nil {
		return user
	}

	effectBet, profitLoss, count, account, isTourist := RedisIns().GetUserStatData(userID)
	user = &User{
		TotalEffectBet: effectBet,
		TotalProfLoss:  profitLoss,
		Count:          count,
		UserId:         userID,
		AgentId:        ad.Id,
		Account:        account,
		IsTourist:      isTourist,
	}
	ad.userCache[userID] = user
	return user
}

func (ad *AgentData) GetGame(symbol string) *Game {
	game := ad.gameCache[symbol]
	if game != nil {
		return game
	}

	effectBet, chips, profitLoss, revenue, _ := RedisIns().GetGameStatData(ad.Id, symbol)
	game = &Game{
		TotalEffectBet: effectBet,
		TotalChips:     chips,
		TotalProfLoss:  profitLoss,
		TotalRevenue:   revenue,
		AgentId:        ad.Id,
		Symbol:         symbol,
	}
	ad.gameCache[symbol] = game
	return game
}

// GameCacheMgr 缓存统一资金服务使用的奖池累计值，并定期持久化到 Redis。
type GameCacheMgr struct {
	lock   *sync.RWMutex
	agents map[int64]*AgentData
}

// GetAgent 获取代理缓存；首次访问时创建空缓存，具体累计值按需从 Redis 加载。
func (gcm *GameCacheMgr) GetAgent(agentID int64) *AgentData {
	gcm.lock.Lock()
	defer gcm.lock.Unlock()

	agent := gcm.agents[agentID]
	if agent == nil {
		agent = &AgentData{
			lock:      &sync.RWMutex{},
			Id:        agentID,
			gameCache: make(map[string]*Game),
			userCache: make(map[uint32]*User),
		}
		gcm.agents[agentID] = agent
	}
	return agent
}

// GetPool 返回扣除有效预留后的可用水池：Pool = 有效下注 - 总赔付 - 税收 - 有效预留。
func (gcm *GameCacheMgr) GetPool(agentID int64, symbol string) decimal.Decimal {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	game := agent.GetGame(symbol)
	// 预留独立保存在 Redis，确保同一奖池的有效预留不会只存在某个实例内存中。
	reserved := RedisIns().GetAgentReserved(agentID, symbol)
	return availablePoolValue(game, reserved)
}

// poolValue 计算尚未扣除有效预留的基础水池：有效下注 - 总赔付 - 税收。
func poolValue(game *Game) decimal.Decimal {
	return game.TotalEffectBet.Sub(game.TotalProfLoss).Sub(game.TotalRevenue)
}

// availablePoolValue 在基础奖池上扣除当前仍然有效的赔付预留。
func availablePoolValue(game *Game, reserved decimal.Decimal) decimal.Decimal {
	return poolValue(game).Sub(reserved)
}

// poolType 按指定房间 level 的水位配置划分低/中/高水位，并返回对应可赔付比例。
func poolType(pool decimal.Decimal, item *config.PoolItem) (int, decimal.Decimal) {
	if item == nil {
		return config.POOL_BREAK_DOWN, decimal.Zero
	}
	if pool.LessThanOrEqual(decimal.Zero) {
		return config.POOL_BREAK_DOWN, decimal.Zero
	}
	if pool.LessThanOrEqual(item.Normal) {
		return config.POOL_LOW, item.MinRate
	}
	if pool.LessThanOrEqual(item.Max) {
		return config.POOL_NORMAL, item.NormalRate
	}
	return config.POOL_HIGH, item.MaxRate
}

// CanAffordAward 参照旧版 Lottery 的可赔付计算（暂不接入单控/自动单控）：
//
//	p = min(水池余额*水位比例, 人均有效投注*倍数)
//	够赔当且仅当 p >= award
//
// PrePay 场景下本局下注与税收已入池，award 应传入 max(0, 本局赔付-本局总下注)。
// 水位/税率/基数均取 symbol + level 对应的 PoolItem。
func (gcm *GameCacheMgr) CanAffordAward(
	agentID int64,
	userID uint32,
	item *config.PoolItem,
	symbol string,
	level uint32,
	award decimal.Decimal,
) bool {
	if !award.IsPositive() {
		return true
	}
	if item == nil || config.CfgIns == nil {
		zap.L().Debug("CanAffordAward:缺少水池或开奖配置",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.String("award", award.String()),
		)
		return false
	}

	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	user := agent.GetUser(userID)
	game := agent.GetGame(symbol)
	pool := poolValue(game).Add(item.Base)

	t, r := poolType(pool, item)
	if t == config.POOL_BREAK_DOWN {
		zap.L().Debug("CanAffordAward:水池破产",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.String("pool", pool.String()),
			zap.String("award", award.String()),
		)
		return false
	}

	rate := decimal.Zero
	if !user.TotalEffectBet.IsZero() {
		rate = user.TotalProfLoss.Div(user.TotalEffectBet)
	}
	cItems := config.CfgIns.GetAwardOddsConfigWithProfitOdds(symbol, rate)
	if cItems == nil {
		cItems = config.CfgIns.GetAwardOddsDefaultCtrlConfig(symbol)
	}
	if cItems == nil || t < 0 || t >= len(cItems.PoolOdds) || cItems.PoolOdds[t] == nil {
		zap.L().Debug("CanAffordAward:缺少水位开奖配置",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.Int("poolType", t),
			zap.String("rate", rate.String()),
		)
		return false
	}
	oddsItem := cItems.PoolOdds[t]

	p1 := pool.Mul(r)
	cnt := decimal.NewFromInt(1)
	if !user.Count.IsZero() {
		cnt = user.Count.Add(decimal.NewFromInt(1))
	}
	p2 := user.TotalEffectBet.Div(cnt).Mul(oddsItem.M)
	p := p1
	if p1.GreaterThan(p2) {
		p = p2
		zap.L().Debug("CanAffordAward:使用平均值",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.String("payable", p.String()),
			zap.String("award", award.String()),
			zap.String("totalEffectBet", user.TotalEffectBet.String()),
			zap.String("count", user.Count.String()),
			zap.Any("oddsItem", oddsItem),
		)
	} else {
		zap.L().Debug("CanAffordAward:使用水池值",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.String("payable", p.String()),
			zap.String("award", award.String()),
			zap.String("pool", pool.String()),
			zap.String("poolRate", r.String()),
			zap.Any("oddsItem", oddsItem),
		)
	}

	if p.LessThan(award) {
		zap.L().Debug("CanAffordAward:赔付失败",
			zap.Int64("agentId", agentID),
			zap.Uint32("userId", userID),
			zap.String("symbol", symbol),
			zap.Uint32("level", level),
			zap.String("payable", p.String()),
			zap.String("award", award.String()),
		)
		return false
	}
	zap.L().Debug("CanAffordAward:赔付成功",
		zap.Int64("agentId", agentID),
		zap.Uint32("userId", userID),
		zap.String("symbol", symbol),
		zap.Uint32("level", level),
		zap.String("payable", p.String()),
		zap.String("award", award.String()),
	)
	return true
}

func (gcm *GameCacheMgr) GetPlayerAccount(agentID, userID int64) string {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	return agent.GetUser(uint32(userID)).Account
}

// ApplyPoolChange 统一登记下注、赔付和退款引起的奖池增量。
// betDelta 为正表示新增下注、为负表示退款；payoutDelta 为正表示玩家赔付。
func (gcm *GameCacheMgr) ApplyPoolChange(
	agentID int64,
	userID uint32,
	symbol string,
	currencyType string,
	recordID string,
	betDelta decimal.Decimal,
	payoutDelta decimal.Decimal,
	revenueDelta decimal.Decimal,
) {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	user := agent.GetUser(userID)
	if user.IsTourist != 0 {
		return
	}

	game := agent.GetGame(symbol)
	before := poolValue(game)
	game.TotalEffectBet = game.TotalEffectBet.Add(betDelta.Truncate(4))
	// TotalProfLoss 只累计实际赔付，不混入下注或玩家净利润。
	game.TotalProfLoss = game.TotalProfLoss.Add(payoutDelta.Truncate(4))
	game.TotalRevenue = game.TotalRevenue.Add(revenueDelta.Truncate(4))
	game.UpdateTime = time.Now().Unix()

	user.TotalEffectBet = user.TotalEffectBet.Add(betDelta.Truncate(4))
	user.UpdateTime = time.Now().Unix()
	after := poolValue(game)
	zap.L().Debug("pool change",
		zap.Int64("agentId", agentID),
		zap.Uint32("userId", userID),
		zap.String("symbol", symbol),
		zap.String("currencyType", currencyType),
		zap.String("recordId", recordID),
		zap.String("betDelta", betDelta.String()),
		zap.String("payoutDelta", payoutDelta.String()),
		zap.String("revenueDelta", revenueDelta.String()),
		zap.String("before", before.String()),
		zap.String("after", after.String()),
	)
}

// TryReservePool 原子检查基础奖池并建立预留，返回 false 表示奖池不足或 Redis 操作失败。
// Deprecated: 新路径请用 TryReservePoolWithLedger。
func (gcm *GameCacheMgr) TryReservePool(agentID int64, symbol string, amount decimal.Decimal) (bool, error) {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	basePool := poolValue(agent.GetGame(symbol))
	return RedisIns().TryReserveAgentPool(agentID, symbol, basePool, amount)
}

// TryReservePoolWithLedger 建立预留并写入 reservationId ledger。
func (gcm *GameCacheMgr) TryReservePoolWithLedger(
	agentID int64,
	symbol string,
	amount decimal.Decimal,
	reservationID, roundID string,
) (ReserveWithLedgerResult, error) {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	basePool := poolValue(agent.GetGame(symbol))
	return RedisIns().TryReserveAgentPoolWithLedger(
		agentID, symbol, basePool, amount, reservationID, roundID,
	)
}

// ReleasePoolReservation 释放指定牌局占用的有效预留，重复释放会被 Redis 拒绝。
// Deprecated: 新路径请用 ReleasePoolReservationWithLedger。
func (gcm *GameCacheMgr) ReleasePoolReservation(agentID int64, symbol string, amount decimal.Decimal) bool {
	released, err := RedisIns().ReleaseAgentReserved(agentID, symbol, amount)
	if err != nil {
		zap.L().Error("释放奖池有效预留失败", zap.Int64("agentId", agentID), zap.String("symbol", symbol), zap.String("amount", amount.String()), zap.Error(err))
		return false
	}
	if !released {
		zap.L().Error("奖池有效预留不足，拒绝重复释放", zap.Int64("agentId", agentID), zap.String("symbol", symbol), zap.String("amount", amount.String()))
	}
	return released
}

// ReleasePoolReservationWithLedger 按 reservationId 幂等释放预留。
func (gcm *GameCacheMgr) ReleasePoolReservationWithLedger(
	agentID int64,
	symbol string,
	amount decimal.Decimal,
	reservationID, roundID, releaseID, reason string,
	allowMigrate bool,
) ReleaseWithLedgerResult {
	result, err := RedisIns().ReleaseAgentReservedWithLedger(
		agentID, symbol, amount, reservationID, roundID, releaseID, reason, allowMigrate,
	)
	if err != nil {
		zap.L().Error("带 ledger 释放奖池预留失败",
			zap.Int64("agentId", agentID),
			zap.String("symbol", symbol),
			zap.String("amount", amount.String()),
			zap.String("reservationId", reservationID),
			zap.String("releaseId", releaseID),
			zap.Error(err),
		)
		return ReleaseWithLedgerResult{}
	}
	if !result.Ok {
		zap.L().Error("带 ledger 释放奖池预留被拒绝",
			zap.Int64("agentId", agentID),
			zap.String("symbol", symbol),
			zap.String("amount", amount.String()),
			zap.String("reservationId", reservationID),
			zap.String("releaseId", releaseID),
		)
	}
	return result
}

// AdjustPoolReservationWithLedger 调整同一 reservationId 占用额（LIABILITY_CAP）。
func (gcm *GameCacheMgr) AdjustPoolReservationWithLedger(
	agentID int64,
	symbol string,
	oldAmount, newAmount decimal.Decimal,
	reservationID, roundID string,
) (bool, error) {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	basePool := poolValue(agent.GetGame(symbol))
	ok, err := RedisIns().AdjustAgentReservedWithLedger(
		agentID, symbol, basePool, oldAmount, newAmount, reservationID, roundID,
	)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	// ledger 缺失：补建 ACTIVE（不改 Q，旧预留已在汇总里），再调一次。
	if seedErr := RedisIns().SeedReservationLedgerActive(
		agentID, symbol, oldAmount, reservationID, roundID,
	); seedErr != nil {
		return false, seedErr
	}
	return RedisIns().AdjustAgentReservedWithLedger(
		agentID, symbol, basePool, oldAmount, newAmount, reservationID, roundID,
	)
}

// RecordSettlement 更新打码量、玩家局数和玩家累计返奖，不重复修改代理奖池。
func (gcm *GameCacheMgr) RecordSettlement(agentID int64, userID uint32, symbol string, bet, payout decimal.Decimal) {
	agent := gcm.GetAgent(agentID)
	agent.lock.Lock()
	defer agent.lock.Unlock()

	user := agent.GetUser(userID)
	if user.IsTourist != 0 {
		return
	}

	game := agent.GetGame(symbol)
	chips := payout
	if payout.LessThan(bet) {
		chips = bet
	}
	game.TotalChips = game.TotalChips.Add(chips.Truncate(4))
	game.UpdateTime = time.Now().Unix()
	user.Count = user.Count.Add(decimal.NewFromInt(1))
	user.TotalProfLoss = user.TotalProfLoss.Add(payout.Truncate(4))
	user.UpdateTime = time.Now().Unix()
}

// GetAgentChanges 收集尚未持久化的玩家和奖池统计。
func GetAgentChanges() ([]*User, []*Game) {
	gameCache.lock.RLock()
	defer gameCache.lock.RUnlock()

	users := make([]*User, 0, 64)
	games := make([]*Game, 0, 64)
	for _, agent := range gameCache.agents {
		agent.lock.RLock()
		for _, game := range agent.gameCache {
			if game.UpdateTime > 0 {
				games = append(games, game)
			}
		}
		for _, user := range agent.userCache {
			if user.UpdateTime > 0 {
				users = append(users, user)
			}
		}
		agent.lock.RUnlock()
	}
	return users, games
}

// ResetPool 清空内存和 Redis 中的代理奖池累计值。
func ResetPool(_ interface{}) {
	gameCache.lock.Lock()
	defer gameCache.lock.Unlock()

	for _, agent := range gameCache.agents {
		agent.lock.Lock()
		for _, game := range agent.gameCache {
			game.TotalChips = decimal.Zero
			game.TotalEffectBet = decimal.Zero
			game.TotalProfLoss = decimal.Zero
			game.TotalRevenue = decimal.Zero
		}
		agent.lock.Unlock()
	}

	pipeline := RedisIns().cli.Pipeline()
	pipeline.Del(context.Background(), "agent_effect_data")
	pipeline.Del(context.Background(), "agent_chips_data")
	pipeline.Del(context.Background(), "agent_revenue_data")
	pipeline.Del(context.Background(), "agent_profitLoss_data")
	pipeline.Del(context.Background(), "agent_reserved_data")
	if _, err := pipeline.Exec(context.Background()); err != nil {
		zap.L().Error("reset pool failed", zap.Error(err))
	}
}

// SaveAgentData 批量持久化 CNY 奖池累计值，二级键格式为 agentId_symbol_level。
func SaveAgentData(games []*Game) {
	pipeline := RedisIns().cli.Pipeline()
	queued := 0
	for _, game := range games {
		if game.UpdateTime <= 0 {
			continue
		}
		key := fmt.Sprintf("%d_%s", game.AgentId, game.Symbol)
		pipeline.ZAdd(context.Background(), "agent_effect_data", redis.Z{Score: game.TotalEffectBet.InexactFloat64(), Member: key})
		pipeline.ZAdd(context.Background(), "agent_chips_data", redis.Z{Score: game.TotalChips.InexactFloat64(), Member: key})
		pipeline.ZAdd(context.Background(), "agent_profitLoss_data", redis.Z{Score: game.TotalProfLoss.InexactFloat64(), Member: key})
		pipeline.ZAdd(context.Background(), "agent_revenue_data", redis.Z{Score: game.TotalRevenue.InexactFloat64(), Member: key})
		game.UpdateTime = 0
		queued++
		if queued >= 100 {
			execPipeline(pipeline, "save agent finance data")
			pipeline = RedisIns().cli.Pipeline()
			queued = 0
		}
	}
	if queued > 0 {
		execPipeline(pipeline, "save agent finance data")
	}
}

// SaveUserData 批量持久化玩家有效下注、返奖和局数统计。
func SaveUserData(users []*User) {
	pipeline := RedisIns().cli.Pipeline()
	queued := 0
	for _, user := range users {
		if user.UpdateTime <= 0 {
			continue
		}
		key := fmt.Sprintf("%d", user.UserId)
		pipeline.ZAdd(context.Background(), "userTotalEffBet", redis.Z{Score: user.TotalEffectBet.InexactFloat64(), Member: key})
		pipeline.ZAdd(context.Background(), "userTotalProfLoss", redis.Z{Score: user.TotalProfLoss.InexactFloat64(), Member: key})
		pipeline.ZAdd(context.Background(), "userBetCount", redis.Z{Score: user.Count.InexactFloat64(), Member: key})
		user.UpdateTime = 0
		queued++
		if queued >= 100 {
			execPipeline(pipeline, "save user finance data")
			pipeline = RedisIns().cli.Pipeline()
			queued = 0
		}
	}
	if queued > 0 {
		execPipeline(pipeline, "save user finance data")
	}
}

func execPipeline(pipeline redis.Pipeliner, operation string) {
	if _, err := pipeline.Exec(context.Background()); err != nil {
		zap.L().Error(operation+" failed", zap.Error(err))
	}
}

// CacheInit 初始化奖池缓存，并启动统计数据定时落地任务。
func CacheInit() {
	if gameCache != nil {
		return
	}
	gameCache = &GameCacheMgr{
		lock:   &sync.RWMutex{},
		agents: make(map[int64]*AgentData),
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			users, games := GetAgentChanges()
			SaveAgentData(games)
			SaveUserData(users)
		}
	}()
}

func CacheIns() *GameCacheMgr {
	return gameCache
}

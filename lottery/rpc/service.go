package rpc

import (
	"app/config"
	"app/entity"
	"app/entity/view"
	"app/tables/player"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"lottery/dao"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"micro_service/services"

	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic/v7"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	// financeRoundHash 保存全部牌局的资金状态快照。
	financeRoundHash = "lottery_finance_rounds"
	// 未指定预留时长时，默认锁定奖池 300 秒。
	defaultReservationTimeoutSeconds = 300
	financeModeNormal                = "NORMAL"
	financeModeVoidRefund            = "VOID_REFUND"
	// reservationModeLiabilityCap 为单人房发牌前责任上限预留：
	// 可在预留下继续接受下注，结算时实际赔付不得超过该上限。
	reservationModeLiabilityCap = "LIABILITY_CAP"
)

type financeState string

const (
	financeStateBetting  financeState = "BETTING"  // 允许首次下注和追加下注。
	financeStateReserved financeState = "RESERVED" // 候选结果已经成功锁定赔付额度。
	financeStateSettled  financeState = "SETTLED"  // 本局已经完成正常结算。
	financeStateVoided   financeState = "VOIDED"   // 本局下注已经全部原路退回。
	financeStateExpired  financeState = "EXPIRED"  // 赔付预留已超时自动释放。
)

// financeBetSnapshot 固化单笔下注结果，用于下注幂等和摘要计算。
type financeBetSnapshot struct {
	BetID        string `json:"betId"`
	UserID       uint32 `json:"userId"`
	CurrencyType string `json:"currencyType"`
	Amount       string `json:"amount"`
	AmountCNY    string `json:"amountCny"`
	AreaID       string `json:"areaId,omitempty"`
	Accepted     bool   `json:"accepted"`
	Code         int32  `json:"code"`
	Currency     string `json:"currency,omitempty"`
	Message      string `json:"message,omitempty"`
}

// financeReservation 保存一次候选结果对应的奖池预留凭证。
type financeReservation struct {
	RequestID      string `json:"requestId"`
	ReservationID  string `json:"reservationId"`
	BetDigest      string `json:"betDigest"`
	OutcomeHash    string `json:"outcomeHash"`
	TotalPayoutCNY string `json:"totalPayoutCny"`
	ExpiresAt      int64  `json:"expiresAt"`
	Status         string `json:"status"`
	// Mode 为空表示精确结果预留；LIABILITY_CAP 表示责任上限预留。
	Mode string `json:"mode,omitempty"`
}

type financeSettlementResult struct {
	UserID   uint32 `json:"userId"`
	Code     int32  `json:"code"`
	Currency string `json:"currency,omitempty"`
	Message  string `json:"message,omitempty"`
}

// financeSettlement 固化最终结算结果，保证 settlementId 重试不会重复入账。
type financeSettlement struct {
	SettlementID string                     `json:"settlementId"`
	Reservation  string                     `json:"reservationId"`
	BetDigest    string                     `json:"betDigest"`
	OutcomeHash  string                     `json:"outcomeHash"`
	Mode         string                     `json:"mode"`
	Results      []*financeSettlementResult `json:"results"`
	CompletedAt  int64                      `json:"completedAt"`
}

// financeRound 是一个 roundId 的完整资金状态机快照。
type financeRound struct {
	RoundID     string                         `json:"roundId"`
	GameID      uint32                         `json:"gameId"`
	Agent       uint32                         `json:"agent"`
	Level       uint32                         `json:"level"`
	Symbol      string                         `json:"symbol"`
	PoolSymbol  string                         `json:"poolSymbol"`
	State       string                         `json:"state"`
	Bets        map[string]*financeBetSnapshot `json:"bets"`
	Reservation *financeReservation            `json:"reservation,omitempty"`
	Settlement  *financeSettlement             `json:"settlement,omitempty"`
	UpdatedAt   int64                          `json:"updatedAt"`
}

type RecordItem struct {
	record  *entity.CacheRecordsReq
	TimeOut int64
}

type RecordCacheMgr struct {
	lock    *sync.RWMutex
	records map[string]*RecordItem
}

// LotteryService 实现统一资金 RPC，并维护牌局状态、账单和注单异步落地队列。
type LotteryService struct {
	services.UnimplementedLotteryServiceServer
	db  *dao.DBDao
	rds *dao.RedisDao
	es  *dao.ESDao

	pcr        *PoolChangeRecord
	poolChange chan *view.PoolLogItem
	RecordChan chan *entity.CacheRecordsReq
	BillChan   chan *entity.CacheBillsReq

	recordsCache *RecordCacheMgr

	roundLock *sync.RWMutex
	rounds    map[string]*financeRound
}

type PoolChangeRecord struct {
	lock   *sync.RWMutex
	record map[string]decimal.Decimal
}

// roundRuntime 是根据 agent、gameId 和 level 解析出的可信运行参数。
type roundRuntime struct {
	AgentID    uint32
	GameID     uint32
	Level      uint32
	WebID      uint32
	Symbol     string
	PoolSymbol string
	Revenue    decimal.Decimal
}

// decimalFromCent 将 Redis 中以“分”为单位的整数余额转换为十进制金额。
func decimalFromCent(v int64) decimal.Decimal {
	return decimal.NewFromInt(v).Div(decimal.NewFromInt(100))
}

func normalizeFinanceMode(mode string) string {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return financeModeNormal
	}
	return mode
}

// buildPoolSymbol 生成 symbol_level，最终 Redis 二级键为 agentId_symbol_level。
func buildPoolSymbol(symbol string, level uint32) string {
	return fmt.Sprintf("%s_%d", symbol, level)
}

// baseSymbolFromPoolSymbol 从奖池标识中还原游戏 symbol，供配置和日志查询使用。
func baseSymbolFromPoolSymbol(symbol string) string {
	separator := strings.LastIndex(symbol, "_")
	if separator < 0 {
		return symbol
	}
	if _, err := strconv.ParseUint(symbol[separator+1:], 10, 32); err != nil {
		return symbol
	}
	return symbol[:separator]
}

func roundBetRecordID(roundID, betID string) string {
	return fmt.Sprintf("%s#%s", roundID, betID)
}

func decimalString(value decimal.Decimal) string {
	return value.Truncate(4).String()
}

// parseMoney 解析非负金额，并限制到玩家账户支持的分精度。
func parseMoney(raw string) (decimal.Decimal, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return decimal.Zero, fmt.Errorf("amount is required")
	}
	value, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, err
	}
	if value.LessThan(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("amount cannot be negative")
	}
	if !value.Equal(decimalFromCent(toCentDelta(value))) {
		return decimal.Zero, fmt.Errorf("amount cannot have precision below one cent")
	}
	return value, nil
}

// parseSignedMoney 用于解析允许为负数的净利润字段。
func parseSignedMoney(raw string) (decimal.Decimal, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return decimal.Zero, fmt.Errorf("amount is required")
	}
	value, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, err
	}
	if !value.Equal(decimalFromCent(toCentDelta(value))) {
		return decimal.Zero, fmt.Errorf("amount cannot have precision below one cent")
	}
	return value, nil
}

// toCentDelta 将已校验分精度的金额转换为 Redis 原子增减使用的整数。
func toCentDelta(value decimal.Decimal) int64 {
	return value.Mul(decimal.NewFromInt(100)).Truncate(0).IntPart()
}

func errorCode(code int32) services.ErrorCode {
	return services.ErrorCode(code)
}

func ConvertUserEntityToHumanPlayer(p *player.Player) *services.HumanPlayer {
	return &services.HumanPlayer{
		Id:             uint32(p.UserId),
		Nickname:       p.NickName,
		GameCurrency:   p.Score.StringFixed(2),
		Avatar:         p.Pic,
		Gender:         uint32(p.Sex),
		Exp:            p.Exp,
		AgentId:        uint32(p.ProxyId),
		LoginIP:        p.LoginIp,
		LoginTimeStamp: p.LoginTime,
		CurrencyLimit:  p.MoneyLimit.StringFixed(2),
		WebSiteId:      uint32(p.WebsiteId),
		Account:        p.Account,
		CurrencyType:   p.CurrencyType,
		AllTimes:       p.AllTimes,
		IsTourist:      p.IsTourist,
	}
}

func (p *PoolChangeRecord) Record(agentId int64, symbol string, value decimal.Decimal) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.record[fmt.Sprintf("%d-%s", agentId, symbol)] = value
}

// NewLotteryService 初始化统一资金服务，并恢复 Redis 中尚未清理的牌局状态。
func NewLotteryService(es *elastic.Client) *LotteryService {
	service := &LotteryService{
		db:         dao.NewDBDao(),
		rds:        dao.RedisIns(),
		es:         dao.NewESDao(es),
		poolChange: make(chan *view.PoolLogItem, 10240*5),
		RecordChan: make(chan *entity.CacheRecordsReq, 10240*5),
		BillChan:   make(chan *entity.CacheBillsReq, 10240*5),
		pcr: &PoolChangeRecord{
			lock:   &sync.RWMutex{},
			record: make(map[string]decimal.Decimal),
		},
		roundLock: &sync.RWMutex{},
		rounds:    make(map[string]*financeRound),
	}
	service.initRecordsCache()
	service.loadRoundStates()
	service.startRoundExpirationLoop()
	service.producterPoolLog()
	service.consumerPool()
	service.consumerRecord()
	service.consumerBill()
	return service
}

// startRoundExpirationLoop 定期把超时预留从 RESERVED 原子推进到 EXPIRED。
func (d *LotteryService) startRoundExpirationLoop() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			d.roundLock.Lock()
			for _, round := range d.rounds {
				d.expireRoundIfNeededLocked(round)
			}
			d.roundLock.Unlock()
		}
	}()
}

// loadRoundStates 在服务启动时恢复牌局，支持热更新和实例迁移后的状态查询。
func (d *LotteryService) loadRoundStates() {
	if d.rds == nil {
		return
	}
	data, err := d.rds.HGetAll(financeRoundHash)
	if err != nil {
		zap.L().Error("load finance rounds failed", zap.Error(err))
		return
	}
	d.roundLock.Lock()
	defer d.roundLock.Unlock()
	for roundID, raw := range data {
		round := &financeRound{}
		if err := jsoniter.UnmarshalFromString(raw, round); err != nil {
			zap.L().Error("decode finance round failed", zap.String("roundId", roundID), zap.Error(err))
			continue
		}
		if round.Bets == nil {
			round.Bets = make(map[string]*financeBetSnapshot)
		}
		d.rounds[round.RoundID] = round
	}
}

func (d *LotteryService) loadPlayerToCache(id uint32) services.ErrorCode {
	playerInfo, err := d.db.GetPlayer(id)
	if err != nil {
		zap.L().Error("load player from db failed", zap.Any("id", id), zap.Error(err))
		return services.ErrorCode_SYSTEM_ERROR
	}
	if err := d.rds.SetPlayer(ConvertUserEntityToHumanPlayer(playerInfo)); err != nil {
		zap.L().Error("set player cache failed", zap.Any("id", id), zap.Error(err))
		return services.ErrorCode_SYSTEM_ERROR
	}
	return services.ErrorCode_OK
}

func (d *LotteryService) getPlayerCurrency(id uint32) (int64, services.ErrorCode) {
	newCurrency, err := d.rds.GetPlayerCurrency(id)
	if err == nil {
		return newCurrency, services.ErrorCode_OK
	}
	if err == redis.Nil {
		if code := d.loadPlayerToCache(id); code != services.ErrorCode_OK {
			return 0, code
		}
		newCurrency, err = d.rds.GetPlayerCurrency(id)
		if err == nil {
			return newCurrency, services.ErrorCode_OK
		}
	}
	zap.L().Error("get player currency failed", zap.Any("id", id), zap.Error(err))
	return 0, services.ErrorCode_SYSTEM_ERROR
}

func (d *LotteryService) updatePlayerCurrency(id uint32, delta int64) (int64, services.ErrorCode) {
	newCurrency, err := d.rds.UpdatePlayerCurrency(id, delta, 0, 0, 0)
	if err != nil {
		if errors.Is(err, dao.ErrInsufficientFunds) {
			return newCurrency, services.ErrorCode_NO_ENOUGH_MONEY
		}
		if errors.Is(err, dao.ErrPlayerNotCached) {
			if code := d.loadPlayerToCache(id); code != services.ErrorCode_OK {
				return 0, code
			}
			newCurrency, err = d.rds.UpdatePlayerCurrency(id, delta, 0, 0, 0)
			if err != nil {
				if errors.Is(err, dao.ErrInsufficientFunds) {
					return newCurrency, services.ErrorCode_NO_ENOUGH_MONEY
				}
				zap.L().Error("update player currency failed", zap.Any("id", id), zap.Error(err))
				return 0, services.ErrorCode_SYSTEM_ERROR
			}
			return newCurrency, services.ErrorCode_OK
		}
		zap.L().Error("update player currency failed", zap.Any("id", id), zap.Error(err))
		return 0, services.ErrorCode_SYSTEM_ERROR
	}
	return newCurrency, services.ErrorCode_OK
}

func (d *LotteryService) SaveBill(agentId, playerId uint32, delta decimal.Decimal, currencyScore float64, symbol, desc, currencyType, roundID string) {
	now := time.Now()
	billNo := fmt.Sprintf("L%04d%02d%02d%02d%02d%02d%07d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond()%10000000)
	eGame := dao.GamesManagerIns().Get(symbol)
	bill := &entity.CacheBillsReq{
		UserId:         playerId,
		GameId:         uint32(eGame.Number),
		AgentId:        agentId,
		Bet:            delta.InexactFloat64(),
		CurrentScore:   currencyScore,
		Currency:       currencyType,
		CreateTime:     now.Unix(),
		RoundID:        roundID,
		FlowingWaterOn: billNo,
		Symbol:         symbol,
		Desc:           desc,
	}
	d.BillChan <- bill
}

func (d *LotteryService) SaveRecord(record *entity.CacheRecordsReq) *entity.CacheRecordsReq {
	d.recordsCache.lock.Lock()
	defer d.recordsCache.lock.Unlock()

	hashStr := fmt.Sprintf("%d|%d|%s", record.AgentId, record.UserId, record.RoundID)
	record.Hash = fmt.Sprintf("%x", md5.Sum([]byte(hashStr)))
	d.recordsCache.records[record.Hash] = &RecordItem{
		TimeOut: time.Now().Unix() + 10,
		record:  record,
	}
	return record
}

func (d *LotteryService) initRecordsCache() {
	if d.recordsCache != nil {
		return
	}
	d.recordsCache = &RecordCacheMgr{
		lock:    &sync.RWMutex{},
		records: make(map[string]*RecordItem),
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			tmp := make([]*entity.CacheRecordsReq, 0, 512)
			d.recordsCache.lock.Lock()
			now := time.Now().Unix()
			for key, item := range d.recordsCache.records {
				tmp = append(tmp, item.record)
				if now > item.TimeOut || item.record.Complete {
					delete(d.recordsCache.records, key)
				}
			}
			d.recordsCache.lock.Unlock()
			for _, item := range tmp {
				d.RecordChan <- item
			}
		}
	}()
}

func (d *LotteryService) consumerPool() {
	gw := &sync.WaitGroup{}
	gw.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zap.L().Error("consumerPool panic", zap.Any("err", err))
			}
		}()
		gw.Done()
		data := make([]*view.PoolLogItem, 0, 64)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(data) > 0 {
					d.BulkPoolLog(data)
					data = make([]*view.PoolLogItem, 0, 64)
				}
			case req := <-d.poolChange:
				data = append(data, req)
				if len(data) >= 32 {
					d.BulkPoolLog(data)
					data = make([]*view.PoolLogItem, 0, 64)
				}
			}
		}
	}()
	gw.Wait()
}

func (d *LotteryService) producterPoolLog() {
	gw := &sync.WaitGroup{}
	gw.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zap.L().Error("producterPoolLog panic", zap.Any("err", err))
			}
		}()
		gw.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			d.pcr.lock.Lock()
			for key, value := range d.pcr.record {
				arr := strings.SplitN(key, "-", 2)
				if len(arr) != 2 {
					continue
				}
				agentID, _ := strconv.ParseInt(arr[0], 10, 64)
				symbol := arr[1]
				baseSymbol := baseSymbolFromPoolSymbol(symbol)
				pcfg := config.CfgIns.GetPoolCfg(agentID, baseSymbol)
				if pcfg == nil {
					continue
				}
				d.poolChange <- &view.PoolLogItem{
					AgentId:    int(agentID),
					Symbol:     symbol,
					PoolValue:  value.Truncate(2).InexactFloat64(),
					Normal:     int(pcfg.Pool[1].Normal.IntPart()),
					NormalRate: pcfg.Pool[1].NormalRate,
					Min:        int(pcfg.Pool[1].Min.IntPart()),
					MinRate:    pcfg.Pool[1].MinRate,
					Max:        int(pcfg.Pool[1].Max.IntPart()),
					MaxRate:    pcfg.Pool[1].MaxRate,
					Ctl:        int(pcfg.Pool[1].Control.IntPart()),
					Revenue:    pcfg.Pool[1].Revenue,
					CreateTime: time.Now().Unix(),
				}
			}
			d.pcr.record = make(map[string]decimal.Decimal)
			d.pcr.lock.Unlock()
		}
	}()
	gw.Wait()
}

func (d *LotteryService) consumerRecord() {
	gw := &sync.WaitGroup{}
	gw.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zap.L().Error("consumerRecord panic", zap.Any("err", err))
			}
		}()
		gw.Done()
		data := make([]*entity.CacheRecordsReq, 0, 64)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(data) > 0 {
					d.es.BulkRecordsSave(data)
					data = make([]*entity.CacheRecordsReq, 0, 64)
				}
			case req := <-d.RecordChan:
				data = append(data, req)
				if len(data) >= 40 {
					d.es.BulkRecordsSave(data)
					data = make([]*entity.CacheRecordsReq, 0, 64)
				}
			}
		}
	}()
	gw.Wait()
}

func (d *LotteryService) consumerBill() {
	gw := &sync.WaitGroup{}
	gw.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zap.L().Error("consumerBill panic", zap.Any("err", err))
			}
		}()
		gw.Done()
		data := make([]*entity.CacheBillsReq, 0, 64)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(data) > 0 {
					d.es.BulkBillsSave(data)
					data = make([]*entity.CacheBillsReq, 0, 64)
				}
			case req := <-d.BillChan:
				data = append(data, req)
				if len(data) >= 50 {
					d.es.BulkBillsSave(data)
					data = make([]*entity.CacheBillsReq, 0, 64)
				}
			}
		}
	}()
	gw.Wait()
}

func (d *LotteryService) BulkPoolLog(data []*view.PoolLogItem) error {
	bulkService := d.es.Client.Bulk()
	records := make([]elastic.BulkableRequest, 0, len(data))
	for _, req := range data {
		records = append(records, elastic.NewBulkIndexRequest().Index("pp_pool_record_log").Doc(req))
	}
	bulkService.Add(records...)
	_, err := bulkService.Do(context.Background())
	if err != nil {
		zap.L().Error("BulkPoolLog failed", zap.Any("err", err))
	}
	return err
}

// ConvertRecord 将结算金额和抽水按玩家币种及 CNY 两种口径写入注单。
func ConvertRecord(agentId, userId uint32, recordID, currencyType, symbol, account, log string, newCurrency decimal.Decimal, webID uint32, complete bool, totalBet, win, pumpAmount float64) *entity.CacheRecordsReq {
	rate, _ := config.CfgIns.GetExchange(currencyType)
	p := config.CfgIns.GetPoolCfg(int64(agentId), symbol)
	bet := decimal.NewFromFloat(totalBet)
	award := decimal.NewFromFloat(win)
	pump := decimal.NewFromFloat(pumpAmount)
	chips := bet
	if chips.LessThan(award) {
		chips = award
	}

	if account == "" {
		account = dao.CacheIns().GetPlayerAccount(int64(agentId), int64(userId))
	}

	revenue := bet.Mul(p.Pool[1].Revenue)
	return &entity.CacheRecordsReq{
		WebId:          webID,
		UserId:         userId,
		AgentId:        agentId,
		GameId:         uint32(p.GameId),
		Account:        account,
		NickName:       account,
		Bet:            bet.Truncate(4).InexactFloat64(),
		ExBet:          bet.Mul(rate).Truncate(4).InexactFloat64(),
		Currency:       currencyType,
		CurrencySymbol: currencyType,
		BaseBet:        totalBet,
		Win:            award.Truncate(4).InexactFloat64(),
		ExWin:          award.Mul(rate).Truncate(4).InexactFloat64(),
		PlayedDate:     time.Now().UnixMilli(),
		RoundID:        recordID,
		Symbol:         symbol,
		RowVersion:     time.Now().UnixNano(),
		Revenue:        revenue.Truncate(4).InexactFloat64(),
		ExRevenue:      revenue.Mul(rate).Truncate(4).InexactFloat64(),
		Pump:           pump.Truncate(4).InexactFloat64(),
		ExPump:         pump.Mul(rate).Truncate(4).InexactFloat64(),
		Log:            log,
		GameName:       p.Name,
		Balance:        newCurrency.Truncate(4).InexactFloat64(),
		BalanceCash:    newCurrency.Truncate(4).InexactFloat64(),
		Chips:          chips.Mul(rate).Truncate(4).InexactFloat64(),
		Complete:       complete,
	}
}

func (d *LotteryService) roundStateCode(round *financeRound) string {
	if round == nil || round.State == "" {
		return string(financeStateBetting)
	}
	return round.State
}

// loadRoundLocked 优先读取内存，未命中时再从 Redis 恢复。调用方必须持有 roundLock。
func (d *LotteryService) loadRoundLocked(roundID string) (*financeRound, bool) {
	if round, ok := d.rounds[roundID]; ok {
		return round, true
	}
	if d.rds == nil {
		return nil, false
	}
	raw, err := d.rds.HGet(financeRoundHash, roundID)
	if err != nil {
		return nil, false
	}
	round := &financeRound{}
	if err := jsoniter.UnmarshalFromString(raw, round); err != nil {
		zap.L().Error("decode finance round failed", zap.String("roundId", roundID), zap.Error(err))
		return nil, false
	}
	if round.Bets == nil {
		round.Bets = make(map[string]*financeBetSnapshot)
	}
	d.rounds[round.RoundID] = round
	return round, true
}

// saveRoundLocked 同步更新内存和 Redis 中的牌局资金快照。调用方必须持有 roundLock。
func (d *LotteryService) saveRoundLocked(round *financeRound) {
	if round.Bets == nil {
		round.Bets = make(map[string]*financeBetSnapshot)
	}
	round.UpdatedAt = time.Now().Unix()
	d.rounds[round.RoundID] = round
	raw, err := jsoniter.MarshalToString(round)
	if err != nil {
		zap.L().Error("encode finance round failed", zap.String("roundId", round.RoundID), zap.Error(err))
		return
	}
	if err := d.rds.HSet(financeRoundHash, raw, round.RoundID); err != nil {
		zap.L().Error("persist finance round failed", zap.String("roundId", round.RoundID), zap.Error(err))
	}
}

// expireRoundIfNeededLocked 释放已到期预留，使其不再占用可用奖池。
func (d *LotteryService) expireRoundIfNeededLocked(round *financeRound) bool {
	if round == nil || round.Reservation == nil {
		return false
	}
	if round.Reservation.Status != string(financeStateReserved) {
		return false
	}
	if round.Reservation.ExpiresAt <= 0 || time.Now().Unix() < round.Reservation.ExpiresAt {
		return false
	}
	amount, err := decimal.NewFromString(round.Reservation.TotalPayoutCNY)
	if err != nil {
		zap.L().Error("解析过期预留金额失败", zap.String("roundId", round.RoundID), zap.Error(err))
		return false
	}
	if !dao.CacheIns().ReleasePoolReservation(int64(round.Agent), round.PoolSymbol, amount) {
		return false
	}
	round.Reservation.Status = string(financeStateExpired)
	round.State = string(financeStateExpired)
	d.saveRoundLocked(round)
	return true
}

// roundBetDigest 对排序后的全部已接受下注计算 SHA-256，锁定结算使用的订单集合。
func (d *LotteryService) roundBetDigest(round *financeRound) string {
	if round == nil || len(round.Bets) == 0 {
		return ""
	}
	keys := make([]string, 0, len(round.Bets))
	for betID, item := range round.Bets {
		if item != nil && item.Accepted {
			keys = append(keys, betID)
		}
	}
	sort.Strings(keys)
	payload := make([]string, 0, len(keys))
	for _, betID := range keys {
		item := round.Bets[betID]
		payload = append(payload, strings.Join([]string{
			item.BetID,
			strconv.FormatUint(uint64(item.UserID), 10),
			item.CurrencyType,
			item.Amount,
			item.AreaID,
		}, "|"))
	}
	sum := sha256.Sum256([]byte(strings.Join(payload, "\n")))
	return hex.EncodeToString(sum[:])
}

// totalBetCNY 汇总本局已接受下注换算后的 CNY 金额。
func (d *LotteryService) totalBetCNY(round *financeRound) decimal.Decimal {
	total := decimal.Zero
	if round == nil {
		return total
	}
	for _, item := range round.Bets {
		if item == nil || !item.Accepted {
			continue
		}
		amount, err := decimal.NewFromString(item.AmountCNY)
		if err == nil {
			total = total.Add(amount)
		}
	}
	return total
}

// resolveRuntime 校验代理和游戏状态，并解析 symbol、汇率配置所需的可信路由信息。
func (d *LotteryService) resolveRuntime(agentID, gameID, level uint32) (*roundRuntime, services.ErrorCode) {
	agent := dao.AgentManagerIns().Get(int64(agentID))
	if agent == nil || agent.IsFrozen == 1 {
		return nil, services.ErrorCode_AGENT_FROZEN
	}
	game := dao.GamesManagerIns().GetById(int64(gameID))
	if game == nil || game.Number != int(gameID) {
		return nil, services.ErrorCode_PARAMS_INVALID
	}
	if game.IsFrozen == 1 {
		return nil, services.ErrorCode_GAME_FROZEN
	}
	poolCfg := config.CfgIns.GetPoolCfg(int64(agentID), game.ConfName)
	if poolCfg == nil {
		return nil, services.ErrorCode_SYSTEM_ERROR
	}
	return &roundRuntime{
		AgentID:    agentID,
		GameID:     gameID,
		Level:      level,
		WebID:      uint32(agent.WebId),
		Symbol:     game.ConfName,
		PoolSymbol: buildPoolSymbol(game.ConfName, level),
		Revenue:    poolCfg.Pool[1].Revenue,
	}, services.ErrorCode_OK
}

// validateRound 防止跨代理、跨游戏或跨等级复用同一个 roundId。
func (d *LotteryService) validateRound(round *financeRound, runtime *roundRuntime) services.ErrorCode {
	if round == nil {
		return services.ErrorCode_OK
	}
	if round.Agent != runtime.AgentID || round.GameID != runtime.GameID || round.Level != runtime.Level || round.PoolSymbol != runtime.PoolSymbol {
		return services.ErrorCode_PARAMS_INVALID
	}
	return services.ErrorCode_OK
}

func (d *LotteryService) buildBetResponse(item *financeBetSnapshot) *services.BetItemResult {
	if item == nil {
		return nil
	}
	return &services.BetItemResult{
		BetId:    item.BetID,
		UserId:   item.UserID,
		Code:     errorCode(item.Code),
		Accepted: item.Accepted,
		Currency: item.Currency,
		Message:  item.Message,
	}
}

func (d *LotteryService) buildSettlementResponse(item *financeSettlementResult) *services.SettlementItemResult {
	if item == nil {
		return nil
	}
	return &services.SettlementItemResult{
		UserId:   item.UserID,
		Code:     errorCode(item.Code),
		Currency: item.Currency,
		Message:  item.Message,
	}
}

// Bet 原子扣除玩家余额并登记下注；相同 roundId + betId 重试直接返回首次结果。
func (d *LotteryService) Bet(_ context.Context, req *services.BetRequest) (resp *services.BetResponse, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			zap.L().Error("Bet panic", zap.Any("err", rec))
			resp = &services.BetResponse{Code: services.ErrorCode_SYSTEM_ERROR}
			err = nil
		}
	}()

	resp = &services.BetResponse{
		Code:    services.ErrorCode_OK,
		RoundId: req.RoundId,
		State:   string(financeStateBetting),
		Items:   make([]*services.BetItemResult, 0, len(req.Items)),
	}
	if req.RequestId == "" || req.RoundId == "" || req.GameId == 0 || req.Agent == 0 || len(req.Items) == 0 {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	runtime, code := d.resolveRuntime(req.Agent, req.GameId, req.Level)
	if code != services.ErrorCode_OK {
		resp.Code = code
		return resp, nil
	}

	d.roundLock.Lock()
	defer d.roundLock.Unlock()

	round, exists := d.loadRoundLocked(req.RoundId)
	if exists {
		if code := d.validateRound(round, runtime); code != services.ErrorCode_OK {
			resp.Code = code
			return resp, nil
		}
		d.expireRoundIfNeededLocked(round)
	} else {
		round = &financeRound{
			RoundID:    req.RoundId,
			GameID:     req.GameId,
			Agent:      req.Agent,
			Level:      req.Level,
			Symbol:     runtime.Symbol,
			PoolSymbol: runtime.PoolSymbol,
			State:      string(financeStateBetting),
			Bets:       make(map[string]*financeBetSnapshot),
		}
	}

	shouldSave := !exists
	for _, item := range req.Items {
		if item == nil {
			continue
		}
		if snapshot, ok := round.Bets[item.BetId]; ok {
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}
		snapshot := &financeBetSnapshot{
			BetID:        item.BetId,
			UserID:       item.UserId,
			CurrencyType: item.CurrencyType,
			Amount:       item.Amount,
			AreaID:       item.AreaId,
			Accepted:     false,
			Code:         int32(services.ErrorCode_PARAMS_INVALID),
		}
		if !d.roundAcceptsBets(round) {
			snapshot.Message = "round is not accepting bets"
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}
		if item.BetId == "" || item.UserId == 0 || item.CurrencyType == "" {
			snapshot.Message = "bet item is invalid"
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}
		amount, parseErr := parseMoney(item.Amount)
		if parseErr != nil || !amount.GreaterThan(decimal.Zero) {
			snapshot.Message = "amount is invalid"
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}
		exchange, ok := config.CfgIns.GetExchange(item.CurrencyType)
		if !ok {
			snapshot.Code = int32(services.ErrorCode_SYSTEM_ERROR)
			snapshot.Message = "exchange config not found"
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}
		delta := -toCentDelta(amount)
		// Redis 脚本在一次原子操作中完成余额存在性、非负校验和扣款。
		newCurrency, currencyCode := d.updatePlayerCurrency(item.UserId, delta)
		if currencyCode != services.ErrorCode_OK {
			snapshot.Code = int32(currencyCode)
			snapshot.Message = "balance update failed"
			resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
			continue
		}

		amountCNY := amount.Mul(exchange).Truncate(4)
		snapshot.Amount = decimalString(amount)
		snapshot.AmountCNY = decimalString(amountCNY)
		snapshot.Accepted = true
		snapshot.Code = int32(services.ErrorCode_OK)
		snapshot.Currency = decimalFromCent(newCurrency).Truncate(2).String()
		round.Bets[item.BetId] = snapshot
		shouldSave = true

		revenueCNY := amountCNY.Mul(runtime.Revenue).Truncate(4)
		dao.CacheIns().ApplyPoolChange(int64(req.Agent), item.UserId, runtime.PoolSymbol, item.CurrencyType, roundBetRecordID(req.RoundId, item.BetId), amountCNY, decimal.Zero, revenueCNY)
		d.SaveBill(req.Agent, item.UserId, amount.Neg(), decimalFromCent(newCurrency).Truncate(2).InexactFloat64(), runtime.Symbol, "bet", item.CurrencyType, req.RoundId)
		d.pcr.Record(int64(req.Agent), runtime.PoolSymbol, dao.CacheIns().GetPool(int64(req.Agent), runtime.PoolSymbol))

		resp.Items = append(resp.Items, d.buildBetResponse(snapshot))
	}

	// 责任上限预留下允许继续下注：同步刷新预留绑定的 betDigest，便于后续结算校验。
	if shouldSave && d.isActiveLiabilityCap(round) {
		round.Reservation.BetDigest = d.roundBetDigest(round)
	}

	resp.State = d.roundStateCode(round)
	resp.BetDigest = d.roundBetDigest(round)
	if shouldSave {
		d.saveRoundLocked(round)
	}
	return resp, nil
}

// PrePay 校验完整下注集合，并在可用奖池中为候选结果建立限时赔付预留。
func (d *LotteryService) PrePay(_ context.Context, req *services.PrePayRequest) (resp *services.PrePayResponse, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			zap.L().Error("PrePay panic", zap.Any("err", rec))
			resp = &services.PrePayResponse{Code: services.ErrorCode_SYSTEM_ERROR}
			err = nil
		}
	}()

	resp = &services.PrePayResponse{
		Code:        services.ErrorCode_OK,
		RoundId:     req.RoundId,
		State:       string(financeStateBetting),
		OutcomeHash: req.OutcomeHash,
	}
	if req.RequestId == "" || req.RoundId == "" || req.GameId == 0 || req.Agent == 0 || req.BetDigest == "" || len(req.Items) == 0 {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}

	runtime, code := d.resolveRuntime(req.Agent, req.GameId, req.Level)
	if code != services.ErrorCode_OK {
		resp.Code = code
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}

	d.roundLock.Lock()
	defer d.roundLock.Unlock()

	round, ok := d.loadRoundLocked(req.RoundId)
	if !ok {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}
	if code := d.validateRound(round, runtime); code != services.ErrorCode_OK {
		resp.Code = code
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}
	d.expireRoundIfNeededLocked(round)

	digest := d.roundBetDigest(round)
	resp.State = d.roundStateCode(round)
	resp.BetDigest = digest
	if digest == "" {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}
	if req.BetDigest != digest {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "BET_MISMATCH"
		return resp, nil
	}
	if round.State == string(financeStateSettled) || round.State == string(financeStateVoided) {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "ROUND_CONFLICT"
		return resp, nil
	}
	reservationMode := strings.TrimSpace(req.ReservationMode)
	if round.Reservation != nil && round.Reservation.Status == string(financeStateReserved) {
		// 精确预留：betDigest + outcomeHash 一致时幂等返回。
		if round.Reservation.OutcomeHash == req.OutcomeHash && round.Reservation.BetDigest == digest {
			resp.Success = true
			resp.State = round.State
			resp.ReservationId = round.Reservation.ReservationID
			resp.TotalPayoutCny = round.Reservation.TotalPayoutCNY
			resp.ExpiresAt = round.Reservation.ExpiresAt
			resp.ReservationMode = round.Reservation.Mode
			resp.Reason = ""
			return resp, nil
		}
		// 责任上限预留：允许换牌后用新 outcomeHash / 新上限重试，或在上限内建立精确预留。
		if round.Reservation.Mode != reservationModeLiabilityCap {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			resp.Reason = "ROUND_CONFLICT"
			return resp, nil
		}
	}
	if req.OutcomeHash == "" {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "INVALID_REQUEST"
		return resp, nil
	}

	// 预赔必须覆盖本局全部真人账户，遗漏或重复账户都会改变真实赔付总额。
	validUsers := make(map[string]struct{})
	for _, item := range round.Bets {
		if item == nil || !item.Accepted {
			continue
		}
		key := fmt.Sprintf("%d|%s", item.UserID, item.CurrencyType)
		validUsers[key] = struct{}{}
	}

	totalPayoutCNY := decimal.Zero
	providedUsers := make(map[string]struct{}, len(req.Items))
	for _, item := range req.Items {
		if item == nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			resp.Reason = "INVALID_REQUEST"
			return resp, nil
		}
		key := fmt.Sprintf("%d|%s", item.UserId, item.CurrencyType)
		if _, ok := validUsers[key]; !ok {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			resp.Reason = "INVALID_REQUEST"
			return resp, nil
		}
		if _, duplicate := providedUsers[key]; duplicate {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			resp.Reason = "INVALID_REQUEST"
			return resp, nil
		}
		providedUsers[key] = struct{}{}
		payout, parseErr := parseMoney(item.Payout)
		if parseErr != nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			resp.Reason = "INVALID_REQUEST"
			return resp, nil
		}
		exchange, ok := config.CfgIns.GetExchange(item.CurrencyType)
		if !ok {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			resp.Reason = "INVALID_REQUEST"
			return resp, nil
		}
		totalPayoutCNY = totalPayoutCNY.Add(payout.Mul(exchange).Truncate(4))
	}
	if len(providedUsers) != len(validUsers) {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		resp.Reason = "BET_MISMATCH"
		return resp, nil
	}
	resp.TotalPayoutCny = decimalString(totalPayoutCNY)

	// 已有责任上限预留时，按差额调整奖池占用，而不是直接冲突拒绝。
	if round.Reservation != nil &&
		round.Reservation.Status == string(financeStateReserved) &&
		round.Reservation.Mode == reservationModeLiabilityCap {
		oldAmount, parseErr := decimal.NewFromString(round.Reservation.TotalPayoutCNY)
		if parseErr != nil {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			resp.Reason = "RESERVATION_PERSIST_FAILED"
			return resp, nil
		}
		delta := totalPayoutCNY.Sub(oldAmount)
		if delta.IsPositive() {
			reserved, reserveErr := dao.CacheIns().TryReservePool(int64(req.Agent), runtime.PoolSymbol, delta)
			if reserveErr != nil {
				resp.Code = services.ErrorCode_SYSTEM_ERROR
				resp.Reason = "RESERVATION_PERSIST_FAILED"
				return resp, nil
			}
			if !reserved {
				resp.Code = services.ErrorCode_NO_ENOUGH_POOL_MONEY
				resp.Reason = "INSUFFICIENT_POOL"
				return resp, nil
			}
		} else if delta.IsNegative() {
			if !dao.CacheIns().ReleasePoolReservation(int64(req.Agent), runtime.PoolSymbol, delta.Abs()) {
				resp.Code = services.ErrorCode_SYSTEM_ERROR
				resp.Reason = "RESERVATION_PERSIST_FAILED"
				return resp, nil
			}
		}
		timeoutSeconds := req.TimeoutSeconds
		if timeoutSeconds == 0 {
			timeoutSeconds = defaultReservationTimeoutSeconds
		}
		round.Reservation.RequestID = req.RequestId
		round.Reservation.BetDigest = digest
		round.Reservation.OutcomeHash = req.OutcomeHash
		round.Reservation.TotalPayoutCNY = decimalString(totalPayoutCNY)
		round.Reservation.ExpiresAt = time.Now().Add(time.Duration(timeoutSeconds) * time.Second).Unix()
		// 精确预留会锁住后续下注；责任上限预留保持可继续下注。
		if reservationMode == reservationModeLiabilityCap {
			round.Reservation.Mode = reservationModeLiabilityCap
			round.State = string(financeStateReserved)
		} else {
			round.Reservation.Mode = ""
			round.State = string(financeStateReserved)
		}
		d.saveRoundLocked(round)

		resp.Success = true
		resp.State = round.State
		resp.ReservationId = round.Reservation.ReservationID
		resp.ExpiresAt = round.Reservation.ExpiresAt
		resp.ReservationMode = round.Reservation.Mode
		return resp, nil
	}

	// Redis Lua 在一次操作中检查基础奖池、扣除已有预留并建立本次预留。
	reserved, reserveErr := dao.CacheIns().TryReservePool(int64(req.Agent), runtime.PoolSymbol, totalPayoutCNY)
	if reserveErr != nil {
		resp.Code = services.ErrorCode_SYSTEM_ERROR
		resp.Reason = "RESERVATION_PERSIST_FAILED"
		return resp, nil
	}
	if !reserved {
		resp.Code = services.ErrorCode_NO_ENOUGH_POOL_MONEY
		resp.Reason = "INSUFFICIENT_POOL"
		return resp, nil
	}

	timeoutSeconds := req.TimeoutSeconds
	if timeoutSeconds == 0 {
		timeoutSeconds = defaultReservationTimeoutSeconds
	}
	round.Reservation = &financeReservation{
		RequestID:      req.RequestId,
		ReservationID:  fmt.Sprintf("%s-%d", req.RoundId, time.Now().UnixNano()),
		BetDigest:      digest,
		OutcomeHash:    req.OutcomeHash,
		TotalPayoutCNY: decimalString(totalPayoutCNY),
		ExpiresAt:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second).Unix(),
		Status:         string(financeStateReserved),
		Mode:           reservationMode,
	}
	round.State = string(financeStateReserved)
	d.saveRoundLocked(round)

	resp.Success = true
	resp.State = round.State
	resp.ReservationId = round.Reservation.ReservationID
	resp.ExpiresAt = round.Reservation.ExpiresAt
	resp.ReservationMode = round.Reservation.Mode
	return resp, nil
}

type settlementItemRuntime struct {
	userID       uint32
	currencyType string
	betAmount    decimal.Decimal
	payout       decimal.Decimal
	profit       decimal.Decimal
	pump         decimal.Decimal
	record       string
	exchange     decimal.Decimal
}

// Settlement 批量关闭本局全部真人订单；NORMAL 赔付，VOID_REFUND 全额退回下注。
func (d *LotteryService) Settlement(_ context.Context, req *services.SettlementRequest) (resp *services.SettlementResponse, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			zap.L().Error("Settlement panic", zap.Any("err", rec))
			resp = &services.SettlementResponse{Code: services.ErrorCode_SYSTEM_ERROR}
			err = nil
		}
	}()

	resp = &services.SettlementResponse{
		Code:         services.ErrorCode_OK,
		RoundId:      req.RoundId,
		SettlementId: req.SettlementId,
		Items:        make([]*services.SettlementItemResult, 0, len(req.Items)),
	}
	if req.RoundId == "" || req.GameId == 0 || req.Agent == 0 || req.SettlementId == "" || len(req.Items) == 0 {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	runtime, code := d.resolveRuntime(req.Agent, req.GameId, req.Level)
	if code != services.ErrorCode_OK {
		resp.Code = code
		return resp, nil
	}
	mode := normalizeFinanceMode(req.Mode)
	if mode != financeModeNormal && mode != financeModeVoidRefund {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	d.roundLock.Lock()
	defer d.roundLock.Unlock()

	round, ok := d.loadRoundLocked(req.RoundId)
	if !ok {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}
	if code := d.validateRound(round, runtime); code != services.ErrorCode_OK {
		resp.Code = code
		return resp, nil
	}
	d.expireRoundIfNeededLocked(round)

	if round.Settlement != nil && round.Settlement.SettlementID == req.SettlementId {
		resp.State = round.State
		for _, item := range round.Settlement.Results {
			resp.Items = append(resp.Items, d.buildSettlementResponse(item))
		}
		return resp, nil
	}
	if round.State == string(financeStateSettled) || round.State == string(financeStateVoided) {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	digest := d.roundBetDigest(round)
	if digest == "" || req.BetDigest != digest {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	if mode == financeModeNormal {
		if round.Reservation == nil || round.Reservation.Status != string(financeStateReserved) {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		if req.ReservationId == "" || req.ReservationId != round.Reservation.ReservationID || req.OutcomeHash == "" {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		// 责任上限预留允许最终结算 outcomeHash 与发牌前预留不同，
		// 实际赔付不超过预留上限即可；精确预留仍要求 outcomeHash 完全一致。
		if round.Reservation.Mode == reservationModeLiabilityCap {
			// 校验放到汇总 payout 之后。
		} else if req.OutcomeHash != round.Reservation.OutcomeHash {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
	} else if round.Reservation != nil && round.Reservation.Status == string(financeStateReserved) {
		if req.ReservationId != round.Reservation.ReservationID {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		if round.Reservation.Mode != reservationModeLiabilityCap && req.OutcomeHash != round.Reservation.OutcomeHash {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
	}

	// 先按玩家和币种汇总已接受下注，结算请求必须与该集合完全一致。
	expectedBets := make(map[string]decimal.Decimal)
	for _, item := range round.Bets {
		if item == nil || !item.Accepted {
			continue
		}
		amount, parseErr := decimal.NewFromString(item.Amount)
		if parseErr != nil {
			continue
		}
		key := fmt.Sprintf("%d|%s", item.UserID, item.CurrencyType)
		expectedBets[key] = expectedBets[key].Add(amount)
	}

	itemRuntimes := make([]*settlementItemRuntime, 0, len(req.Items))
	deltas := make(map[uint32]int64)
	providedBets := make(map[string]struct{}, len(req.Items))
	totalPayoutCNY := decimal.Zero
	for _, item := range req.Items {
		if item == nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		betAmount, betErr := parseMoney(item.BetAmount)
		if betErr != nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		payoutRaw := item.Payout
		if mode == financeModeVoidRefund && strings.TrimSpace(payoutRaw) == "" {
			payoutRaw = item.BetAmount
		}
		payout, payoutErr := parseMoney(payoutRaw)
		if payoutErr != nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		profit, profitErr := parseSignedMoney(item.Profit)
		if strings.TrimSpace(item.Profit) == "" {
			profit = payout.Sub(betAmount)
		} else if profitErr != nil {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		expectedProfit := payout.Sub(betAmount)
		if !profit.Equal(expectedProfit) {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		pump, pumpErr := parseMoney(item.Pump)
		if pumpErr != nil || (mode == financeModeVoidRefund && !pump.IsZero()) {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		if mode == financeModeVoidRefund && !payout.Equal(betAmount) {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		key := fmt.Sprintf("%d|%s", item.UserId, item.CurrencyType)
		expectedBet, ok := expectedBets[key]
		if !ok || !expectedBet.Equal(betAmount) {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		if _, duplicate := providedBets[key]; duplicate {
			resp.Code = services.ErrorCode_PARAMS_INVALID
			return resp, nil
		}
		providedBets[key] = struct{}{}
		exchange, ok := config.CfgIns.GetExchange(item.CurrencyType)
		if !ok {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			return resp, nil
		}
		itemRuntime := &settlementItemRuntime{
			userID:       item.UserId,
			currencyType: item.CurrencyType,
			betAmount:    betAmount,
			payout:       payout,
			profit:       profit,
			pump:         pump,
			record:       item.Record,
			exchange:     exchange,
		}
		itemRuntimes = append(itemRuntimes, itemRuntime)
		totalPayoutCNY = totalPayoutCNY.Add(payout.Mul(exchange).Truncate(4))
		if payout.GreaterThan(decimal.Zero) {
			deltas[item.UserId] += toCentDelta(payout)
		}
	}
	if len(providedBets) != len(expectedBets) {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	if mode == financeModeNormal && round.Reservation != nil {
		reserved, parseErr := decimal.NewFromString(round.Reservation.TotalPayoutCNY)
		if parseErr != nil {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			return resp, nil
		}
		if totalPayoutCNY.GreaterThan(reserved) {
			resp.Code = services.ErrorCode_NO_ENOUGH_POOL_MONEY
			return resp, nil
		}
	}

	// 批量入账前确保玩家缓存完整，避免长牌局缓存过期后创建只有余额字段的残缺 Hash。
	currentCurrencys := make(map[uint32]int64, len(itemRuntimes))
	for _, item := range itemRuntimes {
		if _, loaded := currentCurrencys[item.userID]; loaded {
			continue
		}
		current, currencyCode := d.getPlayerCurrency(item.userID)
		if currencyCode != services.ErrorCode_OK {
			resp.Code = currencyCode
			return resp, nil
		}
		currentCurrencys[item.userID] = current
	}

	// 所有请求项校验完成后才批量入账，校验失败不会产生部分赔付。
	newCurrencys := make(map[uint32]int64, len(deltas))
	if len(deltas) > 0 {
		tmp, batchErr := d.rds.BatchUpdatePlayerCurrencys(deltas)
		if batchErr != nil {
			zap.L().Error("batch settlement balance update failed", zap.Error(batchErr))
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			return resp, nil
		}
		newCurrencys = tmp
		if len(newCurrencys) != len(deltas) {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			return resp, nil
		}
	}

	results := make([]*financeSettlementResult, 0, len(itemRuntimes))
	for _, item := range itemRuntimes {
		currentCurrency, ok := newCurrencys[item.userID]
		if !ok {
			currentCurrency = currentCurrencys[item.userID]
		}
		newCurrency := decimalFromCent(currentCurrency).Truncate(2)
		account := dao.CacheIns().GetPlayerAccount(int64(req.Agent), int64(item.userID))
		record := ConvertRecord(req.Agent, item.userID, req.RoundId, item.currencyType, runtime.Symbol, account, item.record, newCurrency, runtime.WebID, true, item.betAmount.InexactFloat64(), item.payout.InexactFloat64(), item.pump.InexactFloat64())
		d.SaveRecord(record)
		if item.payout.GreaterThan(decimal.Zero) {
			desc := "settlement"
			if mode == financeModeVoidRefund {
				desc = "void_refund"
			}
			d.SaveBill(req.Agent, item.userID, item.payout, newCurrency.InexactFloat64(), runtime.Symbol, desc, item.currencyType, req.RoundId)
		}
		betCNY := item.betAmount.Mul(item.exchange).Truncate(4)
		payoutCNY := item.payout.Mul(item.exchange).Truncate(4)
		if mode == financeModeVoidRefund {
			revenueCNY := betCNY.Mul(runtime.Revenue).Truncate(4)
			dao.CacheIns().ApplyPoolChange(int64(req.Agent), item.userID, runtime.PoolSymbol, item.currencyType, req.RoundId, betCNY.Neg(), decimal.Zero, revenueCNY.Neg())
		} else {
			dao.CacheIns().ApplyPoolChange(int64(req.Agent), item.userID, runtime.PoolSymbol, item.currencyType, req.RoundId, decimal.Zero, payoutCNY, decimal.Zero)
			dao.CacheIns().RecordSettlement(int64(req.Agent), item.userID, runtime.PoolSymbol, betCNY, payoutCNY)
		}
		result := &financeSettlementResult{
			UserID:   item.userID,
			Code:     int32(services.ErrorCode_OK),
			Currency: newCurrency.String(),
		}
		results = append(results, result)
		resp.Items = append(resp.Items, d.buildSettlementResponse(result))
	}

	if round.Reservation != nil && round.Reservation.Status == string(financeStateReserved) {
		reserved, parseErr := decimal.NewFromString(round.Reservation.TotalPayoutCNY)
		if parseErr != nil || !dao.CacheIns().ReleasePoolReservation(int64(round.Agent), round.PoolSymbol, reserved) {
			resp.Code = services.ErrorCode_SYSTEM_ERROR
			return resp, nil
		}
	}

	if mode == financeModeVoidRefund {
		if round.Reservation != nil && round.Reservation.Status == string(financeStateReserved) {
			round.Reservation.Status = string(financeStateVoided)
		}
		round.State = string(financeStateVoided)
	} else {
		if round.Reservation != nil {
			round.Reservation.Status = string(financeStateSettled)
		}
		round.State = string(financeStateSettled)
	}
	round.Settlement = &financeSettlement{
		SettlementID: req.SettlementId,
		Reservation:  req.ReservationId,
		BetDigest:    digest,
		OutcomeHash:  req.OutcomeHash,
		Mode:         mode,
		Results:      results,
		CompletedAt:  time.Now().Unix(),
	}
	d.saveRoundLocked(round)
	d.pcr.Record(int64(req.Agent), runtime.PoolSymbol, dao.CacheIns().GetPool(int64(req.Agent), runtime.PoolSymbol))

	resp.State = round.State
	return resp, nil
}

// GetRoundFinanceState 返回可用于超时重试和故障恢复的牌局资金快照。
func (d *LotteryService) GetRoundFinanceState(_ context.Context, req *services.GetRoundFinanceStateReq) (resp *services.GetRoundFinanceStateResp, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			zap.L().Error("GetRoundFinanceState panic", zap.Any("err", rec))
			resp = &services.GetRoundFinanceStateResp{Code: services.ErrorCode_SYSTEM_ERROR}
			err = nil
		}
	}()

	resp = &services.GetRoundFinanceStateResp{
		Code:    services.ErrorCode_OK,
		RoundId: req.RoundId,
	}
	if req.RoundId == "" {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}

	d.roundLock.Lock()
	defer d.roundLock.Unlock()

	round, ok := d.loadRoundLocked(req.RoundId)
	if !ok {
		resp.Code = services.ErrorCode_PARAMS_INVALID
		return resp, nil
	}
	d.expireRoundIfNeededLocked(round)

	resp.State = round.State
	resp.BetDigest = d.roundBetDigest(round)
	resp.TotalBetCny = decimalString(d.totalBetCNY(round))
	if round.Reservation != nil {
		resp.ReservationId = round.Reservation.ReservationID
		resp.OutcomeHash = round.Reservation.OutcomeHash
		resp.ExpiresAt = round.Reservation.ExpiresAt
		resp.ReservationMode = round.Reservation.Mode
		if round.Reservation.Status == string(financeStateReserved) {
			resp.TotalReservedCny = round.Reservation.TotalPayoutCNY
		} else {
			resp.TotalReservedCny = "0"
		}
	}
	if round.Settlement != nil {
		resp.SettlementId = round.Settlement.SettlementID
	}
	return resp, nil
}

func (d *LotteryService) isActiveLiabilityCap(round *financeRound) bool {
	return round != nil &&
		round.Reservation != nil &&
		round.Reservation.Status == string(financeStateReserved) &&
		round.Reservation.Mode == reservationModeLiabilityCap
}

// roundAcceptsBets：BETTING 可下注；责任上限预留下的 RESERVED 也允许继续加注。
func (d *LotteryService) roundAcceptsBets(round *financeRound) bool {
	if round == nil {
		return false
	}
	if round.State == string(financeStateBetting) {
		return true
	}
	return round.State == string(financeStateReserved) && d.isActiveLiabilityCap(round)
}

package tables

import (
	"app/tables/manager"
	"app/tables/player"
	"time"

	"gorm.io/gorm"
)

// 初始化数据库结构
func InitMysqlDb(m, p *gorm.DB) {
	if !m.Migrator().HasTable(&manager.MsgType{}) {
		m.AutoMigrate(&manager.MsgType{})
		mt := []*manager.MsgType{
			{Id: 1, Title: "游戏公告", Class: 1},
			{Id: 2, Title: "维护公告", Class: 1},
			{Id: 3, Title: "管理消息", Class: 2},
		}
		m.Create(mt)
	}
	if !m.Migrator().HasTable(&manager.SystemConf{}) {
		m.AutoMigrate(&manager.SystemConf{})
		sc := []*manager.SystemConf{
			{Id: 1, SystemState: 1},
		}
		m.Create(sc)
	}

	if !m.Migrator().HasTable(&manager.SystemUser{}) {
		m.AutoMigrate(&manager.SystemUser{})
		su := []*manager.SystemUser{
			{Id: 1, Account: "admin1030", Password: "e10adc3949ba59abbe56e057f20f883e", UType: 1, AgentId: 0, UName: "", CreateTime: time.Now().Unix(), IsForzen: 0},
		}
		m.Create(su)
	}
	if !m.Migrator().HasTable(&manager.Game{}) {
		m.AutoMigrate(&manager.Game{})
		games := []*manager.Game{
			{Name: "bjl", NameZH: "百家乐", Number: 1001, ConfName: "bjl", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "lhd", NameZH: "龙虎斗", Number: 1002, ConfName: "lhd", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "hhdz", NameZH: "红黑大战", Number: 1003, ConfName: "hhdz", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "bcbm", NameZH: "奔驰宝马", Number: 1004, ConfName: "bcbm", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "br_nn", NameZH: "百人牛牛", Number: 1005, ConfName: "br_nn", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "fqzs", NameZH: "飞禽走兽", Number: 1006, ConfName: "fqzs", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "slhb", NameZH: "扫雷红包", Number: 1007, ConfName: "slhb", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "ddx", NameZH: "赌大小", Number: 1009, ConfName: "ddx", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "adbh", NameZH: "安达巴哈", Number: 1010, ConfName: "adbh", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "sud", NameZH: "七上七下", Number: 1011, ConfName: "sud", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "zjh", NameZH: "炸金花", Number: 2001, ConfName: "zjh", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "qz_nn", NameZH: "抢庄牛牛", Number: 2002, ConfName: "qz_nn", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "kp_nn", NameZH: "看牌抢庄牛牛", Number: 2003, ConfName: "kp_nn", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "tb_nn", NameZH: "通比牛牛", Number: 2004, ConfName: "tb_nn", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "jd_ddz", NameZH: "斗地主", Number: 2005, ConfName: "jd_ddz", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "dzpk", NameZH: "德州扑克", Number: 2006, ConfName: "dzpk", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "ermj", NameZH: "二人麻将", Number: 2007, ConfName: "ermj", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "sss", NameZH: "十三水", Number: 2009, ConfName: "sss", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "rummy", NameZH: "拉米", Number: 2012, ConfName: "rummy", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "teenpatti", NameZH: "三张牌", Number: 2013, ConfName: "teenpatti", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "slwh", NameZH: "森林舞会", Number: 1008, ConfName: "slwh", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "yxx", NameZH: "鱼虾蟹", Number: 1014, ConfName: "yxx", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "slhb2", NameZH: "宝箱扫雷(国际版)", Number: 1022, ConfName: "slhb2", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "ysdx", NameZH: "越式大小", Number: 1025, ConfName: "ysdx", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "tb", NameZH: "骰宝", Number: 1024, ConfName: "tb", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "roulette", NameZH: "轮盘", Number: 1035, ConfName: "roulette", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "ksznn", NameZH: "看三张抢庄牛牛", Number: 2010, ConfName: "ksznn", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "chjk", NameZH: "免佣21点", Number: 2014, ConfName: "chjk", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "qzsg", NameZH: "抢庄三公", Number: 2022, ConfName: "qzsg", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
			{Name: "ebg", NameZH: "二八杠", Number: 2023, ConfName: "ebg", GameClass: 1, GameType: 3, LimitTime: "10", IsFrozen: 0, State: 1, CreateTime: int(time.Now().Unix()), UpdateTime: int(time.Now().Unix()), Weight: 0, ShowType: 1, IsShow: 1},
		}
		m.Create(games)
	}
	//manager
	m.AutoMigrate(&manager.Agent{},
		&manager.AgentConfig{},
		&manager.AgentGame{},
		&manager.AgentGameConf{},
		&manager.ApiConfig{},
		&manager.Feedback{},
		&manager.GameDataHour{},
		&manager.GameDataSummary{},
		&manager.Log{},
		&manager.Msg{},
		&manager.MsgType{},
		&manager.PlayerDataHour{},
		&manager.PlayerDataSummary{},
		&manager.PlayerProRank{},
		&manager.ProfitLoos{},
		&manager.Statistics{},
		&manager.SystemUserMsg{},
		&manager.User{},
		&manager.Web{},
		&manager.UserScoreLog{},
	)
	//player
	p.AutoMigrate(
		&player.Player{},
	)
}

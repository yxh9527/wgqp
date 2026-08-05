package dao

import (
	"app/config"
	"app/entity"
	"context"
	"crypto/md5"
	"fmt"

	"github.com/olivere/elastic/v7"
	"go.uber.org/zap"
)

func InitES(c *config.RunConfig) (*elastic.Client, error) {
	strs := []string{}
	strs = append(strs, c.Elastic.Host...)
	if client, err := elastic.NewClient(elastic.SetURL(strs...), elastic.SetBasicAuth(c.Elastic.UserName, c.Elastic.Password), elastic.SetSniff(false)); err == nil {
		return client, nil
	} else {
		zap.L().Error("创建es客户端失败", zap.Error(err), zap.Any("strs", c.Elastic), zap.Any("strs", strs))
		return nil, err
	}
}

type ESDao struct {
	Client *elastic.Client
}

func NewESDao(client *elastic.Client) *ESDao {
	return &ESDao{Client: client}
}

func (esDao *ESDao) BulkRecordsSave(data []*entity.CacheRecordsReq) error {
	bulkService := esDao.Client.Bulk()
	records := make([]elastic.BulkableRequest, 0)
	for _, req := range data {
		hashStr := fmt.Sprintf("%d|%d|%s", req.AgentId, req.UserId, req.RoundID)
		req.Hash = fmt.Sprintf("%x", md5.Sum([]byte(hashStr)))
		records = append(records, elastic.NewBulkIndexRequest().Index("pp_gp_settlement").Id(req.Hash).Doc(req))
	}
	bulkService.Add(records...)
	_, err := bulkService.Do(context.Background())
	if err != nil {
		zap.L().Error("BulkRecordsSave,批量插入数据失败", zap.Any("err", err), zap.Any("data", data))
	}
	return nil
}

func (esDao *ESDao) BulkBillsSave(data []*entity.CacheBillsReq) error {
	bulkService := esDao.Client.Bulk()
	records := make([]elastic.BulkableRequest, 0)
	for _, req := range data {
		records = append(records, elastic.NewBulkIndexRequest().Index("pp_gp_flowing_water").Doc(req))
	}
	bulkService.Add(records...)
	_, err := bulkService.Do(context.Background())
	if err != nil {
		zap.L().Error("BulkBillsSave,批量插入数据失败", zap.Any("err", err), zap.Any("data", data))
	}
	return nil
}

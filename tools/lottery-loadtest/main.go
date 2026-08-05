package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"micro_service/services"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func randomID(r *rand.Rand, prefix string) string {
	return fmt.Sprintf("%s-%d-%06d", prefix, time.Now().UnixNano(), r.Intn(1_000_000))
}

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:10080", "grpc addr")
		qps         = flag.Int("qps", 100, "requests per second")
		concurrency = flag.Int("concurrency", 10, "worker count")
		duration    = flag.Duration("duration", time.Minute, "test duration")
		agentID     = flag.Uint64("agent-id", 1, "agent id")
		gameID      = flag.Uint64("game-id", 1, "game id")
		level       = flag.Uint64("level", 1, "pool level")
		recordPref  = flag.String("round-prefix", "LOADTEST", "round prefix")
		betAmount   = flag.String("bet", "1", "bet amount")
		currency    = flag.String("currency", "CNY", "currency type")
		timeout     = flag.Duration("timeout", 3*time.Second, "rpc timeout")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	conn.Connect()

	client := services.NewLotteryServiceClient(conn)
	jobs := make(chan struct{}, *concurrency*2)

	var okCount uint64
	var errCount uint64

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + seed))
			for range jobs {
				roundID := randomID(r, *recordPref)
				betID := randomID(r, "BET")
				req := &services.BetRequest{
					RequestId: randomID(r, "REQ"),
					RoundId:   roundID,
					GameId:    uint32(*gameID),
					Agent:     uint32(*agentID),
					Level:     uint32(*level),
					Items: []*services.BetItem{
						{
							BetId:        betID,
							UserId:       uint32(r.Intn(361) + 1),
							CurrencyType: *currency,
							Amount:       *betAmount,
							AreaId:       "loadtest",
						},
					},
				}

				rpcCtx, rpcCancel := context.WithTimeout(context.Background(), *timeout)
				resp, callErr := client.Bet(rpcCtx, req)
				rpcCancel()
				if callErr != nil || resp == nil || resp.Code != services.ErrorCode_OK {
					atomic.AddUint64(&errCount, 1)
					continue
				}
				if len(resp.Items) == 0 || !resp.Items[0].Accepted {
					atomic.AddUint64(&errCount, 1)
					continue
				}
				atomic.AddUint64(&okCount, 1)
			}
		}(int64(i))
	}

	interval := time.Second
	if *qps > 0 {
		interval = time.Second / time.Duration(*qps)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			fmt.Printf("duration=%s ok=%d err=%d qps=%d concurrency=%d\n",
				time.Since(start).Truncate(time.Second), okCount, errCount, *qps, *concurrency)
			return
		case <-ticker.C:
			select {
			case jobs <- struct{}{}:
			default:
			}
		}
	}
}

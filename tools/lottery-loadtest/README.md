# lottery-loadtest

统一资金 `Bet` RPC 压测工具。每次请求都会创建新的 `roundId`，并提交一笔独立下注。

运行示例：

```bash
go run . -addr 127.0.0.1:10080 -qps 200 -concurrency 20 -duration 60s
```

参数：

- `-agent-id`：代理编号，默认 `1`
- `-game-id`：游戏编号，默认 `1`
- `-level`：奖池房间等级，默认 `1`
- `-bet`：单笔下注金额，默认 `1`
- `-round-prefix`：牌局编号前缀，默认 `LOADTEST`
- `-currency`：玩家币种，默认 `CNY`
- `-timeout`：单次 RPC 超时时间，默认 `3s`

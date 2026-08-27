package dao

import (
	"app/config"
	"testing"

	"github.com/shopspring/decimal"
)

func TestPoolValueUsesEffectBetPayoutAndRevenue(t *testing.T) {
	game := &Game{
		TotalEffectBet: decimal.RequireFromString("100"),
		TotalProfLoss:  decimal.RequireFromString("20"),
		TotalRevenue:   decimal.RequireFromString("3"),
	}

	value := poolValue(game)
	if !value.Equal(decimal.RequireFromString("77")) {
		t.Fatalf("poolValue returned %s", value)
	}
}

func TestAvailablePoolValueSubtractsReservation(t *testing.T) {
	game := &Game{
		TotalEffectBet: decimal.RequireFromString("100"),
		TotalProfLoss:  decimal.RequireFromString("20"),
		TotalRevenue:   decimal.RequireFromString("3"),
	}
	reserved := decimal.RequireFromString("12")

	value := availablePoolValue(game, reserved)
	if !value.Equal(decimal.RequireFromString("65")) {
		t.Fatalf("availablePoolValue returned %s", value)
	}
}

func TestPoolTypeLevels(t *testing.T) {
	item := &config.PoolItem{
		Normal:     decimal.RequireFromString("100"),
		Max:        decimal.RequireFromString("200"),
		MinRate:    decimal.RequireFromString("0.1"),
		NormalRate: decimal.RequireFromString("0.3"),
		MaxRate:    decimal.RequireFromString("0.5"),
	}

	t.Run("break down", func(t *testing.T) {
		got, rate := poolType(decimal.Zero, item)
		if got != config.POOL_BREAK_DOWN || !rate.IsZero() {
			t.Fatalf("got type=%d rate=%s", got, rate)
		}
	})
	t.Run("low", func(t *testing.T) {
		got, rate := poolType(decimal.RequireFromString("50"), item)
		if got != config.POOL_LOW || !rate.Equal(decimal.RequireFromString("0.1")) {
			t.Fatalf("got type=%d rate=%s", got, rate)
		}
	})
	t.Run("normal", func(t *testing.T) {
		got, rate := poolType(decimal.RequireFromString("150"), item)
		if got != config.POOL_NORMAL || !rate.Equal(decimal.RequireFromString("0.3")) {
			t.Fatalf("got type=%d rate=%s", got, rate)
		}
	})
	t.Run("high", func(t *testing.T) {
		got, rate := poolType(decimal.RequireFromString("250"), item)
		if got != config.POOL_HIGH || !rate.Equal(decimal.RequireFromString("0.5")) {
			t.Fatalf("got type=%d rate=%s", got, rate)
		}
	})
	t.Run("missing level config", func(t *testing.T) {
		got, rate := poolType(decimal.RequireFromString("50"), nil)
		if got != config.POOL_BREAK_DOWN || !rate.IsZero() {
			t.Fatalf("got type=%d rate=%s", got, rate)
		}
	})
}

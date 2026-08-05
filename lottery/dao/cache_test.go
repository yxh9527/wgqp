package dao

import (
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

package rpc

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseMoneyCentPrecision(t *testing.T) {
	value, err := parseMoney("1.230")
	if err != nil {
		t.Fatalf("parseMoney returned an error: %v", err)
	}
	if !value.Equal(decimal.RequireFromString("1.23")) {
		t.Fatalf("parseMoney returned %s", value)
	}

	if _, err := parseMoney("1.234"); err == nil {
		t.Fatal("parseMoney accepted precision below one cent")
	}
	if _, err := parseMoney("-1.00"); err == nil {
		t.Fatal("parseMoney accepted a negative amount")
	}
}

func TestParseSignedMoneyAllowsNegativeProfit(t *testing.T) {
	value, err := parseSignedMoney("-1.25")
	if err != nil {
		t.Fatalf("parseSignedMoney returned an error: %v", err)
	}
	if !value.Equal(decimal.RequireFromString("-1.25")) {
		t.Fatalf("parseSignedMoney returned %s", value)
	}
}

func TestPoolSymbolIncludesLevel(t *testing.T) {
	poolSymbol := buildPoolSymbol("table_game", 3)
	if poolSymbol != "table_game_3" {
		t.Fatalf("buildPoolSymbol returned %q", poolSymbol)
	}
	if symbol := baseSymbolFromPoolSymbol(poolSymbol); symbol != "table_game" {
		t.Fatalf("baseSymbolFromPoolSymbol returned %q", symbol)
	}
}

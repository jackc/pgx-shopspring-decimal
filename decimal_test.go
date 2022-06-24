package decimal_test

import (
	"context"
	"math"
	"testing"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxdecimal.Register(conn.TypeMap())
	}
}

func TestCodecDecodeValue(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		original := decimal.RequireFromString("1.234")

		rows, err := conn.Query(context.Background(), `select $1::numeric`, original)
		require.NoError(t, err)

		for rows.Next() {
			values, err := rows.Values()
			require.NoError(t, err)

			require.Len(t, values, 1)
			v0, ok := values[0].(decimal.Decimal)
			require.True(t, ok)
			require.Equal(t, original, v0)
		}

		require.NoError(t, rows.Err())

		rows, err = conn.Query(context.Background(), `select $1::numeric`, nil)
		require.NoError(t, err)

		for rows.Next() {
			values, err := rows.Values()
			require.NoError(t, err)

			require.Len(t, values, 1)
			require.Equal(t, nil, values[0])
		}

		require.NoError(t, rows.Err())
	})
}

func TestNaN(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var d decimal.Decimal
		err := conn.QueryRow(context.Background(), `select 'NaN'::numeric`).Scan(&d)
		require.EqualError(t, err, `can't scan into dest[0]: cannot scan NaN into *decimal.Decimal`)
	})
}

func TestArray(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		inputSlice := []decimal.Decimal{}

		for i := 0; i < 10; i++ {
			d := decimal.NewFromInt(int64(i))
			inputSlice = append(inputSlice, d)
		}

		var outputSlice []decimal.Decimal
		err := conn.QueryRow(context.Background(), `select $1::numeric[]`, inputSlice).Scan(&outputSlice)
		require.NoError(t, err)

		require.Equal(t, len(inputSlice), len(outputSlice))
		for i := 0; i < len(inputSlice); i++ {
			require.True(t, outputSlice[i].Equal(inputSlice[i]))
		}
	})
}

func isExpectedEqDecimal(a decimal.Decimal) func(interface{}) bool {
	return func(v interface{}) bool {
		return a.Equal(v.(decimal.Decimal))
	}
}

func isExpectedEqNullDecimal(a decimal.NullDecimal) func(interface{}) bool {
	return func(v interface{}) bool {
		b := v.(decimal.NullDecimal)
		return a.Valid == b.Valid && a.Decimal.Equal(b.Decimal)
	}
}

func TestValueRoundTrip(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "numeric", []pgxtest.ValueRoundTripTest{
		{
			Param:  decimal.RequireFromString("1"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("1")),
		},
		{
			Param:  decimal.RequireFromString("0.000012345"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("0.000012345")),
		},
		{
			Param:  decimal.RequireFromString("123456.123456"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("123456.123456")),
		},
		{
			Param:  decimal.RequireFromString("-1"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-1")),
		},
		{
			Param:  decimal.RequireFromString("-0.000012345"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-0.000012345")),
		},
		{
			Param:  decimal.RequireFromString("-123456.123456"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-123456.123456")),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("1"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("1"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("0.000012345"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("0.000012345"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("123456.123456"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("123456.123456"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-1"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-1"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-0.000012345"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-0.000012345"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-123456.123456"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-123456.123456"), Valid: true}),
		},
	})
}

func TestValueRoundTripFloat8(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []pgxtest.ValueRoundTripTest{
		{
			Param:  decimal.RequireFromString("1"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("1")),
		},
		{
			Param:  decimal.RequireFromString("0.000012345"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("0.000012345")),
		},
		{
			Param:  decimal.RequireFromString("123456.123456"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("123456.123456")),
		},
		{
			Param:  decimal.RequireFromString("-1"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-1")),
		},
		{
			Param:  decimal.RequireFromString("-0.000012345"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-0.000012345")),
		},
		{
			Param:  decimal.RequireFromString("-123456.123456"),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.RequireFromString("-123456.123456")),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("1"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("1"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("0.000012345"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("0.000012345"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("123456.123456"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("123456.123456"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-1"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-1"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-0.000012345"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-0.000012345"), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.RequireFromString("-123456.123456"), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.RequireFromString("-123456.123456"), Valid: true}),
		},
	})
}

func TestValueRoundTripInt8(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int8", []pgxtest.ValueRoundTripTest{
		{
			Param:  decimal.NewFromInt(0),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.NewFromInt(0)),
		},
		{
			Param:  decimal.NewFromInt(1),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.NewFromInt(1)),
		},
		{
			Param:  decimal.NewFromInt(math.MaxInt64),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.NewFromInt(math.MaxInt64)),
		},
		{
			Param:  decimal.NewFromInt(-1),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.NewFromInt(-1)),
		},
		{
			Param:  decimal.NewFromInt(math.MinInt64),
			Result: new(decimal.Decimal),
			Test:   isExpectedEqDecimal(decimal.NewFromInt(math.MinInt64)),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.NewFromInt(0), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromInt(0), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.NewFromInt(1), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromInt(1), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.NewFromInt(math.MaxInt64), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromInt(math.MaxInt64), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.NewFromInt(-1), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromInt(-1), Valid: true}),
		},
		{
			Param:  decimal.NullDecimal{Decimal: decimal.NewFromInt(math.MinInt64), Valid: true},
			Result: new(decimal.NullDecimal),
			Test:   isExpectedEqNullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromInt(math.MinInt64), Valid: true}),
		},
	})
}

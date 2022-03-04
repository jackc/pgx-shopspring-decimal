package decimal_test

import (
	"context"
	"testing"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestCodecDecodeValue(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	pgxdecimal.Register(conn.TypeMap())

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
}

func TestArray(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	pgxdecimal.Register(conn.TypeMap())

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
}

package decimal_test

import (
	"context"
	"testing"

	"github.com/jackc/pgtype/testutil"
	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestGetter(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	pgxdecimal.Register(conn.ConnInfo())

	original := decimal.RequireFromString("1.234")

	rows, err := conn.Query(context.Background(), `select $1::numeric`, original)
	require.NoError(t, err)

	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)

		require.Len(t, values, 1)
		v0, ok := values[0].(decimal.Decimal)
		require.True(t, ok)
		require.True(t, v0.Equal(original))
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

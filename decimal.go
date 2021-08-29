package decimal

import (
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/shopspring/decimal"
)

type Decimal decimal.Decimal

func (d *Decimal) DecodeNumeric(n *pgtype.Numeric) error {
	if !n.Valid {
		return fmt.Errorf("cannot decode numeric NULL into %T", d)
	}

	*d = Decimal(decimal.NewFromBigInt(n.Int, n.Exp))
	return nil
}

type NullDecimal decimal.NullDecimal

func (d *NullDecimal) DecodeNumeric(n *pgtype.Numeric) error {
	if n.Valid {
		*d = NullDecimal{Decimal: decimal.NewFromBigInt(n.Int, n.Exp), Valid: true}
	} else {
		*d = NullDecimal{}
	}
	return nil
}

func NumericDecoderWrapper(value interface{}) pgtype.NumericDecoder {
	switch value := value.(type) {
	case *decimal.Decimal:
		return (*Decimal)(value)
	case *decimal.NullDecimal:
		return (*NullDecimal)(value)
	default:
		return nil
	}
}

// Register registers the shopspring/decimal integration with a pgtype.ConnInfo.
func Register(ci *pgtype.ConnInfo) {
	ci.PreferAssignToOverSQLScannerForType(&decimal.Decimal{})
	ci.PreferAssignToOverSQLScannerForType(&decimal.NullDecimal{})
	ci.RegisterDataType(pgtype.DataType{
		Value: &pgtype.Numeric{
			NumericDecoderWrapper: NumericDecoderWrapper,
		},
		Name: "numeric",
		OID:  pgtype.NumericOID,
	})
}

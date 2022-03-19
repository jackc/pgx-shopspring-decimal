package decimal

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

type Decimal decimal.Decimal

func (d *Decimal) ScanNumeric(v pgtype.Numeric) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *decimal.Decimal")
	}

	if v.NaN {
		return fmt.Errorf("cannot scan NaN into *decimal.Decimal")
	}

	if v.InfinityModifier != pgtype.Finite {
		return fmt.Errorf("cannot scan %v into *decimal.Decimal", v.InfinityModifier)
	}

	*d = Decimal(decimal.NewFromBigInt(v.Int, v.Exp))

	return nil
}

func (d Decimal) NumericValue() (pgtype.Numeric, error) {
	dd := decimal.Decimal(d)
	return pgtype.Numeric{Int: dd.Coefficient(), Exp: dd.Exponent(), Valid: true}, nil
}

type NullDecimal decimal.NullDecimal

func (d *NullDecimal) ScanNumeric(v pgtype.Numeric) error {
	if !v.Valid {
		*d = NullDecimal{}
		return nil
	}

	if v.NaN {
		return fmt.Errorf("cannot scan NaN into *decimal.NullDecimal")
	}

	if v.InfinityModifier != pgtype.Finite {
		return fmt.Errorf("cannot scan %v into *decimal.NullDecimal", v.InfinityModifier)
	}

	*d = NullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromBigInt(v.Int, v.Exp), Valid: true})

	return nil
}

func (d NullDecimal) NumericValue() (pgtype.Numeric, error) {
	if !d.Valid {
		return pgtype.Numeric{}, nil
	}

	dd := decimal.Decimal(d.Decimal)
	return pgtype.Numeric{Int: dd.Coefficient(), Exp: dd.Exponent(), Valid: true}, nil
}

func TryWrapNumericEncodePlan(value interface{}) (plan pgtype.WrappedEncodePlanNextSetter, nextValue interface{}, ok bool) {
	switch value := value.(type) {
	case decimal.Decimal:
		return &wrapDecimalEncodePlan{}, Decimal(value), true
	case decimal.NullDecimal:
		return &wrapNullDecimalEncodePlan{}, NullDecimal(value), true
	}

	return nil, nil, false
}

type wrapDecimalEncodePlan struct {
	next pgtype.EncodePlan
}

func (plan *wrapDecimalEncodePlan) SetNext(next pgtype.EncodePlan) { plan.next = next }

func (plan *wrapDecimalEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(Decimal(value.(decimal.Decimal)), buf)
}

type wrapNullDecimalEncodePlan struct {
	next pgtype.EncodePlan
}

func (plan *wrapNullDecimalEncodePlan) SetNext(next pgtype.EncodePlan) { plan.next = next }

func (plan *wrapNullDecimalEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(NullDecimal(value.(decimal.NullDecimal)), buf)
}

func TryWrapNumericScanPlan(target interface{}) (plan pgtype.WrappedScanPlanNextSetter, nextDst interface{}, ok bool) {
	switch target := target.(type) {
	case *decimal.Decimal:
		return &wrapDecimalScanPlan{}, (*Decimal)(target), true
	case *decimal.NullDecimal:
		return &wrapNullDecimalScanPlan{}, (*NullDecimal)(target), true
	}

	return nil, nil, false
}

type wrapDecimalScanPlan struct {
	next pgtype.ScanPlan
}

func (plan *wrapDecimalScanPlan) SetNext(next pgtype.ScanPlan) { plan.next = next }

func (plan *wrapDecimalScanPlan) Scan(src []byte, dst interface{}) error {
	return plan.next.Scan(src, (*Decimal)(dst.(*decimal.Decimal)))
}

type wrapNullDecimalScanPlan struct {
	next pgtype.ScanPlan
}

func (plan *wrapNullDecimalScanPlan) SetNext(next pgtype.ScanPlan) { plan.next = next }

func (plan *wrapNullDecimalScanPlan) Scan(src []byte, dst interface{}) error {
	return plan.next.Scan(src, (*NullDecimal)(dst.(*decimal.NullDecimal)))
}

type NumericCodec struct {
	pgtype.NumericCodec
}

func (NumericCodec) DecodeValue(tm *pgtype.Map, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var target decimal.Decimal
	scanPlan := tm.PlanScan(oid, format, &target)
	if scanPlan == nil {
		return nil, fmt.Errorf("PlanScan did not find a plan")
	}

	err := scanPlan.Scan(src, &target)
	if err != nil {
		return nil, err
	}

	return target, nil
}

// Register registers the shopspring/decimal integration with a pgtype.ConnInfo.
func Register(tm *pgtype.Map) {
	tm.TryWrapEncodePlanFuncs = append([]pgtype.TryWrapEncodePlanFunc{TryWrapNumericEncodePlan}, tm.TryWrapEncodePlanFuncs...)
	tm.TryWrapScanPlanFuncs = append([]pgtype.TryWrapScanPlanFunc{TryWrapNumericScanPlan}, tm.TryWrapScanPlanFuncs...)

	tm.RegisterType(&pgtype.Type{
		Name:  "numeric",
		OID:   pgtype.NumericOID,
		Codec: NumericCodec{},
	})
}

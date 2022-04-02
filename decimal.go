package decimal

import (
	"fmt"
	"math"
	"reflect"

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

func (d *Decimal) ScanFloat64(v pgtype.Float8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *decimal.Decimal")
	}

	if math.IsNaN(v.Float64) {
		return fmt.Errorf("cannot scan NaN into *decimal.Decimal")
	}

	if math.IsInf(v.Float64, 0) {
		return fmt.Errorf("cannot scan %v into *decimal.Decimal", v.Float64)
	}

	*d = Decimal(decimal.NewFromFloat(v.Float64))

	return nil
}

func (d Decimal) Float64Value() (pgtype.Float8, error) {
	dd := decimal.Decimal(d)
	return pgtype.Float8{Float64: dd.InexactFloat64(), Valid: true}, nil
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

func (d *NullDecimal) ScanFloat64(v pgtype.Float8) error {
	if !v.Valid {
		*d = NullDecimal{}
		return nil
	}

	if math.IsNaN(v.Float64) {
		return fmt.Errorf("cannot scan NaN into *decimal.NullDecimal")
	}

	if math.IsInf(v.Float64, 0) {
		return fmt.Errorf("cannot scan %v into *decimal.NullDecimal", v.Float64)
	}

	*d = NullDecimal(decimal.NullDecimal{Decimal: decimal.NewFromFloat(v.Float64), Valid: true})

	return nil
}

func (d NullDecimal) Float64Value() (pgtype.Float8, error) {
	if !d.Valid {
		return pgtype.Float8{}, nil
	}

	dd := decimal.NullDecimal(d)
	return pgtype.Float8{Float64: dd.Decimal.InexactFloat64(), Valid: true}, nil
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
func Register(m *pgtype.Map) {
	m.TryWrapEncodePlanFuncs = append([]pgtype.TryWrapEncodePlanFunc{TryWrapNumericEncodePlan}, m.TryWrapEncodePlanFuncs...)
	m.TryWrapScanPlanFuncs = append([]pgtype.TryWrapScanPlanFunc{TryWrapNumericScanPlan}, m.TryWrapScanPlanFuncs...)

	m.RegisterType(&pgtype.Type{
		Name:  "numeric",
		OID:   pgtype.NumericOID,
		Codec: NumericCodec{},
	})

	registerDefaultPgTypeVariants := func(name, arrayName string, value interface{}) {
		// T
		m.RegisterDefaultPgType(value, name)

		// *T
		valueType := reflect.TypeOf(value)
		m.RegisterDefaultPgType(reflect.New(valueType).Interface(), name)

		// []T
		sliceType := reflect.SliceOf(valueType)
		m.RegisterDefaultPgType(reflect.MakeSlice(sliceType, 0, 0).Interface(), arrayName)

		// *[]T
		m.RegisterDefaultPgType(reflect.New(sliceType).Interface(), arrayName)

		// []*T
		sliceOfPointerType := reflect.SliceOf(reflect.TypeOf(reflect.New(valueType).Interface()))
		m.RegisterDefaultPgType(reflect.MakeSlice(sliceOfPointerType, 0, 0).Interface(), arrayName)

		// *[]*T
		m.RegisterDefaultPgType(reflect.New(sliceOfPointerType).Interface(), arrayName)
	}

	registerDefaultPgTypeVariants("numeric", "_numeric", decimal.Decimal{})
	registerDefaultPgTypeVariants("numeric", "_numeric", decimal.NullDecimal{})
	registerDefaultPgTypeVariants("numeric", "_numeric", Decimal{})
	registerDefaultPgTypeVariants("numeric", "_numeric", NullDecimal{})
}

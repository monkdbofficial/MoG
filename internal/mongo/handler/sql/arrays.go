package sql

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

// ArrayArgForSQLType coerces a Mongo/BSON array value into a Go slice value that pgx
// can encode as an array parameter. This avoids relying on the backend to parse JSON
// text into ARRAY(...) values (which is not consistently supported).
func ArrayArgForSQLType(v interface{}, sqlType string) (interface{}, error) {
	if !strings.HasPrefix(sqlType, "ARRAY(") || !strings.HasSuffix(sqlType, ")") {
		return nil, fmt.Errorf("not an ARRAY type: %q", sqlType)
	}
	elemType := strings.TrimSuffix(strings.TrimPrefix(sqlType, "ARRAY("), ")")
	if elemType == "" {
		return nil, fmt.Errorf("invalid ARRAY type: %q", sqlType)
	}

	arr, ok := shared.CoerceInterfaceSlice(v)
	if !ok {
		return nil, fmt.Errorf("value is not an array for %s", sqlType)
	}

	switch elemType {
	case "TEXT", "VARCHAR", "CHAR":
		out := make([]string, 0, len(arr))
		for _, el := range arr {
			if el == nil {
				out = append(out, "")
				continue
			}
			switch t := el.(type) {
			case string:
				out = append(out, t)
			case bson.Symbol:
				out = append(out, string(t))
			case bson.ObjectId:
				out = append(out, t.Hex())
			default:
				out = append(out, fmt.Sprint(el))
			}
		}
		return out, nil
	case "BOOLEAN":
		out := make([]bool, 0, len(arr))
		for _, el := range arr {
			b, ok := el.(bool)
			if !ok {
				return nil, fmt.Errorf("non-boolean element in ARRAY(BOOLEAN): %T", el)
			}
			out = append(out, b)
		}
		return out, nil
	case "BYTE":
		out := make([]int16, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				return nil, fmt.Errorf("non-numeric element in ARRAY(BYTE): %T", el)
			}
			out = append(out, int16(f))
		}
		return out, nil
	case "SHORT":
		out := make([]int16, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				return nil, fmt.Errorf("non-numeric element in ARRAY(SHORT): %T", el)
			}
			out = append(out, int16(f))
		}
		return out, nil
	case "INTEGER":
		out := make([]int32, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				return nil, fmt.Errorf("non-numeric element in ARRAY(INTEGER): %T", el)
			}
			out = append(out, int32(f))
		}
		return out, nil
	case "LONG":
		out := make([]int64, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				switch t := el.(type) {
				case int64:
					out = append(out, t)
					continue
				case int:
					out = append(out, int64(t))
					continue
				default:
					return nil, fmt.Errorf("non-numeric element in ARRAY(LONG): %T", el)
				}
			}
			out = append(out, int64(f))
		}
		return out, nil
	case "FLOAT":
		out := make([]float32, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				return nil, fmt.Errorf("non-numeric element in ARRAY(FLOAT): %T", el)
			}
			out = append(out, float32(f))
		}
		return out, nil
	case "DOUBLE PRECISION":
		out := make([]float64, 0, len(arr))
		for _, el := range arr {
			f, ok := mpipeline.ToFloat64Match(el)
			if !ok {
				return nil, fmt.Errorf("non-numeric element in ARRAY(DOUBLE PRECISION): %T", el)
			}
			out = append(out, f)
		}
		return out, nil
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE":
		out := make([]time.Time, 0, len(arr))
		for _, el := range arr {
			switch t := el.(type) {
			case time.Time:
				out = append(out, t)
			case string:
				if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
					out = append(out, ts)
					continue
				}
				if ts, err := time.Parse(time.RFC3339, t); err == nil {
					out = append(out, ts)
					continue
				}
				return nil, fmt.Errorf("invalid timestamp element in %s: %q", sqlType, t)
			default:
				return nil, fmt.Errorf("non-timestamp element in %s: %T", sqlType, el)
			}
		}
		return out, nil
	case "OBJECT(DYNAMIC)":
		// Encode each object as JSON text and send as TEXT[]; backend casts to OBJECT(DYNAMIC)[].
		out := make([]string, 0, len(arr))
		for _, el := range arr {
			if el == nil {
				out = append(out, "{}")
				continue
			}
			if m, ok := shared.CoerceBsonM(el); ok {
				js, err := shared.MarshalObject(m)
				if err != nil {
					return nil, err
				}
				out = append(out, js)
				continue
			}
			return nil, fmt.Errorf("non-object element in ARRAY(OBJECT(DYNAMIC)): %T", el)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported array element type: %s", elemType)
	}
}


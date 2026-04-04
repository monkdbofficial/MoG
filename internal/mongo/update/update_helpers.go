package update

import (
	"fmt"
	"reflect"
)

// isSliceLike is a helper used by the adapter.
func isSliceLike(v interface{}) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	// Exclude []byte.
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return false
	}
	return true
}

// arrayContains is a helper used by the adapter.
func arrayContains(arr []interface{}, v interface{}) bool {
	for _, el := range arr {
		if reflect.DeepEqual(el, v) || fmt.Sprint(el) == fmt.Sprint(v) {
			return true
		}
	}
	return false
}

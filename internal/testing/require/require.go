package require

import (
	"reflect"
	"testing"
)

func Equal(t *testing.T, x, y any) {
	t.Helper()
	if !reflect.DeepEqual(x, y) {
		t.Fatalf("`%v` != `%v`", x, y)
	}
}

func NotEqual(t *testing.T, x, y any) {
	t.Helper()
	if reflect.DeepEqual(x, y) {
		t.Fatalf("`%v` == `%v`", x, y)
	}
}

func Nil(t *testing.T, x any) {
	t.Helper()
	if !isNil(x) {
		t.Fatalf("expected <nil>, got `%v`", x)
	}
}

func NotNil(t *testing.T, x any) {
	t.Helper()
	if isNil(x) {
		t.Fatalf("expected not <nil>, got `%v`", x)
	}
}

func PanicWithError(t *testing.T, errMsg string, f func()) {
	t.Helper()

	did, msg := didPanic(f)
	if !did {
		t.Fatal("expected panic")
	}
	if msg != errMsg {
		t.Fatalf("expected panic error `%s`, got `%s`", errMsg, msg)
	}
}

func isNil(i any) bool {
	if i == nil {
		return true
	}

	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return v.IsNil()
	}

	return false
}

func didPanic(f func()) (didPanic bool, message any) {
	didPanic = true

	defer func() {
		message = recover()
	}()

	// call the target function
	f()
	didPanic = false

	return
}

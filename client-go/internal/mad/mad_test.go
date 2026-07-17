package mad

import (
	"slices"
	"strings"
	"testing"
)

func TestBasicIntegerTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{"int8", int8(42), int8(42)},
		{"uint8", uint8(255), uint8(255)},
		{"bool_true", true, true},
		{"bool_false", false, false},
		{"int16", int16(-1234), int16(-1234)},
		{"uint16", uint16(65535), uint16(65535)},
		{"int32", int32(-123456), int32(-123456)},
		{"uint32", uint32(4294967295), uint32(4294967295)},
		{"float32", float32(3.14159), float32(3.14159)},
		{"int64", int64(-9223372036854775808), int64(-9223372036854775808)},
		{"uint64", uint64(18446744073709551615), uint64(18446744073709551615)},
		{"float64", float64(3.141592653589793), float64(3.141592653589793)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.value.(type) {
			case int8:
				testRoundTrip(t, v)
			case uint8:
				testRoundTrip(t, v)
			case bool:
				testRoundTrip(t, v)
			case int16:
				testRoundTrip(t, v)
			case uint16:
				testRoundTrip(t, v)
			case int32:
				testRoundTrip(t, v)
			case uint32:
				testRoundTrip(t, v)
			case float32:
				testRoundTrip(t, v)
			case int64:
				testRoundTrip(t, v)
			case uint64:
				testRoundTrip(t, v)
			case float64:
				testRoundTrip(t, v)
			}
		})
	}
}

func testRoundTrip[T comparable](t *testing.T, value T) {
	m, err := NewMad[T]()
	if err != nil {
		t.Fatalf("NewMammd failed: %v", err)
	}

	// Calculate size
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	// Encode
	err = m.Encode(&value, buffer)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	var decoded T
	err = m.Decode(buffer, &decoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Compare
	if decoded != value {
		t.Errorf("Round trip failed: expected %v, got %v", value, decoded)
	}
}

func TestArrayEncoding(t *testing.T) {
	intArray := [3]int32{1, 2, 3}
	testRoundTrip(t, intArray)

	boolArray := [4]bool{true, false, true, false}
	testRoundTrip(t, boolArray)

	floatArray := [2]float64{3.14159, 2.71828}
	testRoundTrip(t, floatArray)
}

func TestBufferTooSmall(t *testing.T) {
	value := int64(12345)
	m, err := NewMad[int64]()
	if err != nil {
		t.Fatalf("NewMammd failed: %v", err)
	}

	// Buffer too small
	smallBuffer := make([]byte, 4) // int64 needs 8 bytes
	err = m.Encode(&value, smallBuffer)
	if err == nil {
		t.Error("Expected error for small buffer, but got none")
	}
}

func TestUnsupportedTypes(t *testing.T) {
	// Test that unsupported types return proper errors
	_, err := NewMad[map[string]int]()
	if err == nil {
		t.Error("Expected error for unsupported map type")
	}

	_, err = NewMad[chan int]()
	if err == nil {
		t.Error("Expected error for unsupported channel type")
	}

	_, err = NewMad[func()]()
	if err == nil {
		t.Error("Expected error for unsupported function type")
	}
}

func TestEmptyStruct(t *testing.T) {
	type EmptyStruct struct{}

	empty := EmptyStruct{}
	testRoundTrip(t, empty)
}

func TestZeroValues(t *testing.T) {
	// Test encoding/decoding zero values
	testRoundTrip(t, int32(0))
	testRoundTrip(t, false)
	testRoundTrip(t, float64(0.0))
}

func TestLargeStruct(t *testing.T) {
	type LargeStruct struct {
		Field01 int64
		Field02 int64
		Field03 float64
		Field04 bool
		Field05 int32
		Field06 int32
		Field07 uint16
		Field08 float32
		Field09 int8
		Field10 uint64
	}

	large := LargeStruct{
		Field01: 1234567890123456789,
		Field02: 987654321098765432,
		Field03: 2.718281828459045,
		Field04: true,
		Field05: -987654321,
		Field06: 123456789,
		Field07: 65535,
		Field08: 1.41421356,
		Field09: -128,
		Field10: 18446744073709551615,
	}

	testRoundTrip(t, large)
}

// Benchmark tests
func BenchmarkEncodeInt32(b *testing.B) {
	m, _ := NewMad[int32]()
	value := int32(42)
	// FIX: Must account for the length of m.Code()
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeInt32(b *testing.B) {
	m, _ := NewMad[int32]()
	value := int32(42)
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	_ = m.Encode(&value, buffer) // FIX: Encode properly to include the header code

	var decoded int32
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

func BenchmarkEncodeSmallArray(b *testing.B) {
	type SmallArray = [4]int32
	m, _ := NewMad[SmallArray]()
	value := SmallArray{1, 2, 3, 4}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeSmallArray(b *testing.B) {
	type SmallArray = [4]int32
	m, _ := NewMad[SmallArray]()
	value := SmallArray{1, 2, 3, 4}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded SmallArray
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

func BenchmarkEncodeMediumArray(b *testing.B) {
	type MediumArray = [100]int32
	m, _ := NewMad[MediumArray]()
	var value MediumArray
	for i := 0; i < 100; i++ {
		value[i] = int32(i)
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeMediumArray(b *testing.B) {
	type MediumArray = [100]int32
	m, _ := NewMad[MediumArray]()
	var value MediumArray
	for i := 0; i < 100; i++ {
		value[i] = int32(i)
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded MediumArray
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

func BenchmarkEncodeLargeArray(b *testing.B) {
	type LargeArray = [1000]int64
	m, _ := NewMad[LargeArray]()
	var value LargeArray
	for i := 0; i < 1000; i++ {
		value[i] = int64(i * i)
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeLargeArray(b *testing.B) {
	type LargeArray = [1000]int64
	m, _ := NewMad[LargeArray]()
	var value LargeArray
	for i := 0; i < 1000; i++ {
		value[i] = int64(i * i)
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded LargeArray
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

// Struct benchmarks
func BenchmarkEncodeSimpleStruct(b *testing.B) {
	type SimpleStruct struct {
		A int32
		B bool
		C float64
	}
	m, _ := NewMad[SimpleStruct]()
	value := SimpleStruct{A: 42, B: true, C: 3.14159}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeSimpleStruct(b *testing.B) {
	type SimpleStruct struct {
		A int32
		B bool
		C float64
	}
	m, _ := NewMad[SimpleStruct]()
	value := SimpleStruct{A: 42, B: true, C: 3.14159}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded SimpleStruct
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

func BenchmarkEncodeNestedStruct(b *testing.B) {
	type Address struct {
		HouseNumber int32
		Country     int32
		ZipCode     int32
	}
	type Person struct {
		ID      int64
		Age     int32
		Height  float32
		Address Address
		Active  bool
	}
	m, _ := NewMad[Person]()
	value := Person{
		ID:     1,
		Age:    30,
		Height: 5.6,
		Address: Address{
			HouseNumber: 42,
			Country:     1,
			ZipCode:     10001,
		},
		Active: true,
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeNestedStruct(b *testing.B) {
	type Address struct {
		HouseNumber int32
		Country     int32
		ZipCode     int32
	}
	type Person struct {
		ID      int64
		Age     int32
		Height  float32
		Address Address
		Active  bool
	}
	m, _ := NewMad[Person]()
	value := Person{
		ID:     1,
		Age:    30,
		Height: 5.6,
		Address: Address{
			HouseNumber: 42,
			Country:     1,
			ZipCode:     10001,
		},
		Active: true,
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded Person
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

func BenchmarkEncodeStructWithArray(b *testing.B) {
	type StructWithArray struct {
		ID     int64
		Scores [10]float32
		Valid  bool
	}
	m, _ := NewMad[StructWithArray]()
	value := StructWithArray{
		ID:     12345,
		Scores: [10]float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0},
		Valid:  true,
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Encode(&value, buffer)
	}
}

func BenchmarkDecodeStructWithArray(b *testing.B) {
	type StructWithArray struct {
		ID     int64
		Scores [10]float32
		Valid  bool
	}
	m, _ := NewMad[StructWithArray]()
	value := StructWithArray{
		ID:     12345,
		Scores: [10]float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0},
		Valid:  true,
	}
	size := m.GetRequiredSize()
	buffer := make([]byte, size)
	m.Encode(&value, buffer)

	var decoded StructWithArray
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Decode(buffer, &decoded)
	}
}

// Test GetRequiredSize method
func TestGetRequiredSize(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		m, err := NewMad[int32]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		size := m.GetRequiredSize()
		expected := 4
		if size != expected {
			t.Errorf("Expected %d bytes for int32, got %d", expected, size)
		}
	})

	t.Run("array", func(t *testing.T) {
		m, err := NewMad[[3]int32]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		size := m.GetRequiredSize()
		expected := 3 * 4
		if size != expected {
			t.Errorf("Expected %d bytes for [3]int32, got %d", expected, size)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type SimpleStruct struct {
			A int32
			B bool
		}
		m, err := NewMad[SimpleStruct]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		size := m.GetRequiredSize()
		expected := 4 + 1
		if size != expected {
			t.Errorf("Expected %d bytes for SimpleStruct, got %d", expected, size)
		}
	})
}

// Test buffer underflow errors
func TestDecoderBufferUnderflow(t *testing.T) {
	t.Run("int64_underflow", func(t *testing.T) {
		m, err := NewMad[int64]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		var result int64
		// Include code "3" but not enough data bytes (need 8, provide only 2)
		buffer := []byte{1, 2}
		err = m.Decode(buffer, &result)
		if err == nil {
			t.Error("Expected buffer underflow error for int64")
		}
		if err != nil && err.Error() != "buffer too small" {
			t.Errorf("Expected 'buffer too small' error, got: %v", err)
		}
	})

	t.Run("int32_underflow", func(t *testing.T) {
		m, err := NewMad[int32]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		var result int32
		// Include code "2" but not enough data bytes (need 4, provide only 2)
		buffer := []byte{1, 2}
		err = m.Decode(buffer, &result)
		if err == nil {
			t.Error("Expected buffer underflow error for int32")
		}
	})

	t.Run("int16_underflow", func(t *testing.T) {
		m, err := NewMad[int16]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		var result int16
		// Include code "1" but not enough data bytes (need 2, provide only 1)
		buffer := []byte{1}
		err = m.Decode(buffer, &result)
		if err == nil {
			t.Error("Expected buffer underflow error for int16")
		}
	})
}

// Test zero-length arrays
func TestZeroLengthArray(t *testing.T) {
	t.Run("zero_int_array", func(t *testing.T) {
		testRoundTrip(t, [0]int32{})
	})

	t.Run("zero_bool_array", func(t *testing.T) {
		testRoundTrip(t, [0]bool{})
	})
}

// Test nested arrays
func TestNestedArrays(t *testing.T) {
	t.Run("2d_int_array", func(t *testing.T) {
		testRoundTrip(t, [2][3]int32{{1, 2, 3}, {4, 5, 6}})
	})

	t.Run("3d_int_array", func(t *testing.T) {
		testRoundTrip(t, [2][2][2]int8{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}})
	})
}

// Test unsupported pointer and interface types
func TestUnsupportedPointerTypes(t *testing.T) {
	_, err := NewMad[*int32]()
	if err == nil {
		t.Error("Should reject pointer types")
	}
	if err != nil && !contains(err.Error(), "unsupported type") {
		t.Errorf("Expected unsupported type error, got: %v", err)
	}
}

func TestUnsupportedInterfaceTypes(t *testing.T) {
	_, err := NewMad[interface{}]()
	if err == nil {
		t.Error("Should reject interface{} types")
	}
}

// Helper function for error message checking
func contains(str, substr string) bool {
	return strings.Contains(str, substr)
}

// Test arrays of structs
func TestArraysOfStructs(t *testing.T) {
	type Point struct {
		X int32
		Y int32
	}

	points := [3]Point{{1, 2}, {3, 4}, {5, 6}}
	testRoundTrip(t, points)
}

// Test deeply nested structs
func TestDeeplyNestedStructs(t *testing.T) {
	type Level3 struct {
		Value int32
	}
	type Level2 struct {
		L3 Level3
		ID int16
	}
	type Level1 struct {
		L2 Level2
		ID int32
	}

	nested := Level1{
		L2: Level2{
			L3: Level3{Value: 42},
			ID: 123,
		},
		ID: 7,
	}

	testRoundTrip(t, nested)
}

// Test struct with array fields
func TestStructWithArrayFields(t *testing.T) {
	type ArrayStruct struct {
		ID     int32
		Scores [5]float32
		Flags  [3]bool
		IDs    [2]int64
	}

	value := ArrayStruct{
		ID:     1,
		Scores: [5]float32{1.1, 2.2, 3.3, 4.4, 5.5},
		Flags:  [3]bool{true, false, true},
		IDs:    [2]int64{1000000000, 2000000000},
	}

	testRoundTrip(t, value)
}

// Test boundary values
func TestBoundaryValues(t *testing.T) {
	t.Run("max_values", func(t *testing.T) {
		testRoundTrip(t, int8(127))
		testRoundTrip(t, int8(-128))
		testRoundTrip(t, uint8(255))
		testRoundTrip(t, int16(32767))
		testRoundTrip(t, int16(-32768))
		testRoundTrip(t, uint16(65535))
		testRoundTrip(t, int32(2147483647))
		testRoundTrip(t, int32(-2147483648))
		testRoundTrip(t, uint32(4294967295))
	})

	t.Run("floating_point_special", func(t *testing.T) {
		testRoundTrip(t, float32(0.0))
		testRoundTrip(t, float32(-0.0))
		testRoundTrip(t, float64(0.0))
		testRoundTrip(t, float64(-0.0))
	})
}

// Test named types vs anonymous types
func TestNamedTypes(t *testing.T) {
	type MyInt int32
	type MyBool bool

	testRoundTrip(t, MyInt(42))
	testRoundTrip(t, MyBool(true))
}

// Test struct field ordering
func TestStructFieldOrdering(t *testing.T) {
	// Fields should be encoded in alphabetical order
	type OrderTest struct {
		Zebra int16
		Alpha int32
		Beta  bool
		Gamma float64
	}

	value := OrderTest{
		Zebra: 99,
		Alpha: 1,
		Beta:  true,
		Gamma: 3.14,
	}

	testRoundTrip(t, value)
}

// Test empty struct edge cases
func TestEmptyStructVariations(t *testing.T) {
	type Empty1 struct{}
	type Empty2 struct{}

	testRoundTrip(t, Empty1{})
	testRoundTrip(t, Empty2{})

	// Array of empty structs
	testRoundTrip(t, [3]Empty1{{}, {}, {}})
}

// Test array element decode error propagation
func TestArrayDecodeErrorPropagation(t *testing.T) {
	m, err := NewMad[[2]int32]()
	if err != nil {
		t.Fatalf("NewMad failed: %v", err)
	}

	var result [2]int32
	// Buffer too small - should cause error in array element decoding
	shortBuffer := []byte{0, 0, 0, 1} // Only 4 bytes, need 8 for [2]int32
	err = m.Decode(shortBuffer, &result)
	if err == nil {
		t.Error("Expected error due to insufficient buffer for array elements")
	}
}

// Test all supported numeric types with their strategies
func TestNumericTypeStrategies(t *testing.T) {
	t.Run("byte_strategy", func(t *testing.T) {
		testRoundTrip(t, int8(42))
		testRoundTrip(t, uint8(255))
		testRoundTrip(t, true)
		testRoundTrip(t, false)
	})

	t.Run("two_byte_strategy", func(t *testing.T) {
		testRoundTrip(t, int16(12345))
		testRoundTrip(t, uint16(54321))
	})

	t.Run("four_byte_strategy", func(t *testing.T) {
		testRoundTrip(t, int32(123456789))
		testRoundTrip(t, uint32(987654321))
		testRoundTrip(t, float32(3.14159))
	})

	t.Run("eight_byte_strategy", func(t *testing.T) {
		testRoundTrip(t, int64(1234567890123456))
		testRoundTrip(t, uint64(9876543210987654))
		testRoundTrip(t, float64(3.141592653589793))
	})
}

// Test complex nested structures
func TestComplexNestedStructures(t *testing.T) {
	type Address struct {
		HouseNumber int32
		ZipCode     int32
	}

	type Person struct {
		ID      int64
		Age     int32
		Address Address
		Scores  [3]float64
		Active  bool
		Balance float64
		Tags    [2]int32
	}

	person := Person{
		ID:  1,
		Age: 30,
		Address: Address{
			HouseNumber: 123,
			ZipCode:     12345,
		},
		Scores:  [3]float64{85.5, 92.0, 78.5},
		Active:  true,
		Balance: 1250.75,
		Tags:    [2]int32{1, 2},
	}

	testRoundTrip(t, person)
}

// Test buffer exact size boundary
func TestBufferExactSizeBoundary(t *testing.T) {
	m, err := NewMad[int64]()
	if err != nil {
		t.Fatalf("NewMad failed: %v", err)
	}

	value := int64(12345)
	exactBuffer := make([]byte, m.GetRequiredSize()) // Exactly the right size

	err = m.Encode(&value, exactBuffer)
	if err != nil {
		t.Fatalf("Encode should succeed with exact buffer size: %v", err)
	}

	var decoded int64
	err = m.Decode(exactBuffer, &decoded)
	if err != nil {
		t.Fatalf("Decode should succeed: %v", err)
	}

	if decoded != value {
		t.Errorf("Expected %d, got %d", value, decoded)
	}
}

// Test error messages consistency
func TestErrorMessagesConsistency(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() error
		expectedMsg string
	}{
		{
			name: "small_buffer_encode",
			setupFunc: func() error {
				m, _ := NewMad[int64]()
				value := int64(123)
				smallBuffer := make([]byte, 4)
				return m.Encode(&value, smallBuffer)
			},
			expectedMsg: "output buffer too small",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupFunc()
			if err == nil {
				t.Error("Expected error but got none")
			}
			if err != nil && err.Error() != tt.expectedMsg {
				t.Errorf("Expected error message %q, got %q", tt.expectedMsg, err.Error())
			}
		})
	}
}

// Test Code() method for hash generation
func TestCodeGeneration(t *testing.T) {
	t.Run("int32_code", func(t *testing.T) {
		m, err := NewMad[int32]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		code := m.Code()
		expected := []byte("b04") // intStrat returns "b0" + size
		if !slices.Equal(code, expected) {
			t.Errorf("Expected code %v for int32, got %v", expected, code)
		}
	})

	t.Run("bool_code", func(t *testing.T) {
		m, err := NewMad[bool]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		code := m.Code()
		expected := []byte("a") // boolStrat returns "a"
		if !slices.Equal(code, expected) {
			t.Errorf("Expected code %v for bool, got %v", expected, code)
		}
	})

	t.Run("array_code", func(t *testing.T) {
		m, err := NewMad[[3]int32]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		code := m.Code()
		expected := []byte("d3b04") // arrStrat returns "d" + len + element code
		if !slices.Equal(code, expected) {
			t.Errorf("Expected code %v for [3]int32, got %v", expected, code)
		}
	})

	t.Run("struct_code", func(t *testing.T) {
		type TestStruct struct {
			A int32
			B bool
		}
		m, err := NewMad[TestStruct]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		code := m.Code()
		// Struct fields are sorted alphabetically: A (int32="b04"), B (bool="a")
		expected := []byte("fb04zaz")
		if !slices.Equal(code, expected) {
			t.Errorf("Expected code %v for TestStruct, got %v", expected, code)
		}
	})

	t.Run("nested_struct_code", func(t *testing.T) {
		type Inner struct {
			X int16
		}
		type Outer struct {
			Inner Inner
			Y     int32
		}
		m, err := NewMad[Outer]()
		if err != nil {
			t.Fatalf("NewMad failed: %v", err)
		}
		code := m.Code()
		// Fields sorted: Inner (struct with int16="b02"), Y (int32="b04")
		expected := []byte("ffb02zzb04z")
		if !slices.Equal(code, expected) {
			t.Errorf("Expected code %v for nested struct, got %v", expected, code)
		}
	})
}

// Test byte strategy buffer underflow to achieve 100% coverage
func TestByteStrategyBufferUnderflow(t *testing.T) {
	m, err := NewMad[int8]()
	if err != nil {
		t.Fatalf("NewMad failed: %v", err)
	}
	var result int8
	var buffer []byte
	err = m.Decode(buffer, &result)
	if err == nil {
		t.Error("Expected buffer underflow error for int8")
	}
	if err != nil && err.Error() != "buffer too small" {
		t.Errorf("Expected 'buffer too small' error, got: %v", err)
	}
}

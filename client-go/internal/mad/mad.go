package mad

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"unsafe"
)

type Mad[T any] struct {
	encoder  func(unsafe.Pointer, *[]byte)
	decoder  func(unsafe.Pointer, *[]byte) error
	sizefunc func() int
	code     []byte
}

func (m *Mad[T]) Code() []byte {
	return m.code
}

func NewMad[T any]() (*Mad[T], error) {
	var zero T

	m := &Mad[T]{}

	encFn, decFn, sizefn, err, hash := generateFuncs(reflect.TypeOf(zero))
	if err != nil {
		return nil, err
	}

	m.encoder = encFn
	m.decoder = decFn
	m.sizefunc = sizefn
	m.code = []byte(hash)
	return m, nil
}

func (m *Mad[T]) GetRequiredSize() int {
	return m.sizefunc()
}

func (m *Mad[T]) Encode(input *T, output []byte) (err error) {
	if len(output) < m.GetRequiredSize() {
		return fmt.Errorf("output buffer too small")
	}
	m.encoder(unsafe.Pointer(input), &output)
	return nil
}

func (m *Mad[T]) Decode(input []byte, output *T) (err error) {
	return m.decoder(unsafe.Pointer(output), &input)
}

func generateFuncs(typ reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, error, string) {
	if typ == nil {
		return nil, nil, nil, fmt.Errorf("unsupported type: <nil>"), ""
	}

	switch typ.Kind() {
	case reflect.Bool:
		enc, dec, size, code := boolStrat()
		return enc, dec, size, nil, code
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		enc, dec, size, code := intStrat(typ)
		return enc, dec, size, nil, code
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		enc, dec, size, code := uintStrat(typ)
		return enc, dec, size, nil, code
	case reflect.Float32, reflect.Float64:
		enc, dec, size, code := floatStrat(typ)
		return enc, dec, size, nil, code
	case reflect.Array:
		return arrStrat(typ)
	case reflect.Struct:
		return structStrat(typ)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported type: %v", typ), ""
	}
}

func boolStrat() (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, string) {
	return func(input unsafe.Pointer, buffer *[]byte) {
			if *(*bool)(input) {
				(*buffer)[0] = 1
			} else {
				(*buffer)[0] = 0
			}
			*buffer = (*buffer)[1:]
		},
		func(output unsafe.Pointer, buffer *[]byte) error {
			if len(*buffer) < 1 {
				return fmt.Errorf("buffer too small")
			}
			*(*bool)(output) = (*buffer)[0] != 0
			*buffer = (*buffer)[1:]
			return nil
		}, func() int {
			return 1
		}, "a"
}

func intStrat(typ reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, string) {
	size := int(typ.Size())
	code := "b0" + strconv.Itoa(size)

	return func(pointer unsafe.Pointer, buffer *[]byte) {
			switch size {
			case 1:
				(*buffer)[0] = *(*byte)(pointer)
			case 2:
				binary.BigEndian.PutUint16((*buffer)[0:2], *(*uint16)(pointer))
			case 4:
				binary.BigEndian.PutUint32((*buffer)[0:4], *(*uint32)(pointer))
			case 8:
				binary.BigEndian.PutUint64((*buffer)[0:8], *(*uint64)(pointer))
			}
			*buffer = (*buffer)[size:]
		}, func(output unsafe.Pointer, buffer *[]byte) error {
			if len(*buffer) < size {
				return fmt.Errorf("buffer too small")
			}
			switch size {
			case 1:
				*(*int8)(output) = *(*int8)(unsafe.Pointer(&((*buffer)[0])))
			case 2:
				*(*uint16)(output) = binary.BigEndian.Uint16((*buffer)[0:2])
			case 4:
				*(*uint32)(output) = binary.BigEndian.Uint32((*buffer)[0:4])
			case 8:
				*(*uint64)(output) = binary.BigEndian.Uint64((*buffer)[0:8])
			}
			*buffer = (*buffer)[size:]
			return nil
		}, func() int {
			return size
		}, code
}

func uintStrat(typ reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, string) {
	size := int(typ.Size())
	code := "b1" + strconv.Itoa(size)

	return func(pointer unsafe.Pointer, buffer *[]byte) {
			switch size {
			case 1:
				(*buffer)[0] = *(*byte)(pointer)
			case 2:
				binary.BigEndian.PutUint16((*buffer)[0:2], *(*uint16)(pointer))
			case 4:
				binary.BigEndian.PutUint32((*buffer)[0:4], *(*uint32)(pointer))
			case 8:
				binary.BigEndian.PutUint64((*buffer)[0:8], *(*uint64)(pointer))
			}
			*buffer = (*buffer)[size:]
		}, func(output unsafe.Pointer, buffer *[]byte) error {
			if len(*buffer) < size {
				return fmt.Errorf("buffer too small")
			}
			switch size {
			case 1:
				*(*uint8)(output) = (*buffer)[0]
			case 2:
				*(*uint16)(output) = binary.BigEndian.Uint16((*buffer)[0:2])
			case 4:
				*(*uint32)(output) = binary.BigEndian.Uint32((*buffer)[0:4])
			case 8:
				*(*uint64)(output) = binary.BigEndian.Uint64((*buffer)[0:8])
			}
			*buffer = (*buffer)[size:]
			return nil
		}, func() int {
			return size
		}, code
}

func floatStrat(typ reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, string) {
	size := int(typ.Size())
	code := "c" + strconv.Itoa(size)

	return func(pointer unsafe.Pointer, buffer *[]byte) {
			if size == 4 {
				bits := math.Float32bits(*(*float32)(pointer))
				binary.BigEndian.PutUint32((*buffer)[0:4], bits)
			} else {
				bits := math.Float64bits(*(*float64)(pointer))
				binary.BigEndian.PutUint64((*buffer)[0:8], bits)
			}
			*buffer = (*buffer)[size:]
		}, func(output unsafe.Pointer, buffer *[]byte) error {
			if len(*buffer) < size {
				return fmt.Errorf("buffer too small")
			}
			if size == 4 {
				bits := binary.BigEndian.Uint32((*buffer)[0:4])
				*(*float32)(output) = math.Float32frombits(bits)
			} else {
				bits := binary.BigEndian.Uint64((*buffer)[0:8])
				*(*float64)(output) = math.Float64frombits(bits)
			}
			*buffer = (*buffer)[size:]
			return nil
		}, func() int {
			return size
		}, code
}

func arrStrat(t reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, error, string) {
	elementType := t.Elem()
	encElemFn, decElemFn, sizeElemFn, err, subCode := generateFuncs(elementType)
	if err != nil {
		return nil, nil, nil, err, ""
	}

	arrLen := t.Len()
	elementSize := elementType.Size()
	code := "d" + strconv.Itoa(arrLen) + subCode

	return func(pointer unsafe.Pointer, buffer *[]byte) {
			for i := 0; i < arrLen; i++ {
				itemPtr := unsafe.Add(pointer, uintptr(i)*elementSize)
				encElemFn(itemPtr, buffer)
			}
		}, func(pointer unsafe.Pointer, buffer *[]byte) error {
			for i := 0; i < arrLen; i++ {
				itemPtr := unsafe.Add(pointer, uintptr(i)*elementSize)
				if err := decElemFn(itemPtr, buffer); err != nil {
					return err
				}
			}
			return nil
		}, func() int {
			return arrLen * sizeElemFn()
		}, nil, code
}

func structStrat(t reflect.Type) (func(unsafe.Pointer, *[]byte), func(unsafe.Pointer, *[]byte) error, func() int, error, string) {
	type fieldMeta struct {
		name   string
		offset uintptr
		enc    func(unsafe.Pointer, *[]byte)
		dec    func(unsafe.Pointer, *[]byte) error
		size   func() int
		code   string
	}

	var fields []fieldMeta
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		encFn, decFn, sizeFn, err, subCode := generateFuncs(f.Type)
		if err != nil {
			return nil, nil, nil, err, ""
		}
		fields = append(fields, fieldMeta{
			offset: f.Offset,
			name:   f.Name,
			enc:    encFn,
			dec:    decFn,
			size:   sizeFn,
			code:   subCode,
		})
	}

	// Alphabetical sort matches the Zig comptime sorting routine
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].name < fields[j].name
	})

	code := "f"
	for i := 0; i < len(fields); i++ {
		code += fields[i].code + "z"
	}

	return func(pointer unsafe.Pointer, buffer *[]byte) {
			for _, field := range fields {
				fieldAddr := unsafe.Add(pointer, field.offset)
				field.enc(fieldAddr, buffer)
			}
		}, func(pointer unsafe.Pointer, buffer *[]byte) error {
			for _, field := range fields {
				fieldAddr := unsafe.Add(pointer, field.offset)
				if err := field.dec(fieldAddr, buffer); err != nil {
					return err
				}
			}
			return nil
		}, func() int {
			total := 0
			for _, field := range fields {
				total += field.size()
			}
			return total
		}, nil, code
}

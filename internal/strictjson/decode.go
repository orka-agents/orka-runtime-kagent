package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"unicode/utf8"
)

var errInvalid = errors.New("invalid JSON")

// Go's ordinary JSON decoder accepts duplicate object members. Reject them at
// every depth before interpreting authority-bearing envelopes or tool arguments.
func Decode(data []byte, value any, strictFields bool) error {
	return decodeJSON(data, value, strictFields, nil)
}

// Remote authority and Responses structs accept single case-folded field names,
// as encoding/json does, but two names must never replace or merge one field.
// Maps, interfaces and custom JSON values remain opaque to field folding.
func DecodeStruct(data []byte, value any, strictFields bool) error {
	return decodeJSON(data, value, strictFields, reflect.TypeOf(value))
}

func decodeJSON(data []byte, value any, strictFields bool, shape reflect.Type) error {
	if !utf8.Valid(data) || !json.Valid(data) || !validStringEscapes(data) {
		return errInvalid
	}
	check := json.NewDecoder(bytes.NewReader(data))
	check.UseNumber()
	if jsonValue(check, 0, shape) != nil {
		return errInvalid
	}
	if _, err := check.Token(); !errors.Is(err, io.EOF) {
		return errInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if strictFields {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(value); err != nil {
		return errInvalid
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Tool arguments
// must retain their exact Unicode value, so reject malformed surrogate pairs.
func validStringEscapes(data []byte) bool {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if data[i] != 'u' {
			continue
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func jsonValue(decoder *json.Decoder, depth int, shape reflect.Type) error {
	if depth > 64 {
		return errInvalid
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		fields, err := structFields(shape, 0)
		if err != nil {
			return err
		}
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errInvalid
			}
			name, fieldType := matchField(fields, name)
			if seen[name] {
				return errInvalid
			}
			seen[name] = true
			if err := jsonValue(decoder, depth+1, fieldType); err != nil {
				return err
			}
		}
	case '[':
		var element reflect.Type
		if shape := structType(shape); shape != nil && (shape.Kind() == reflect.Slice || shape.Kind() == reflect.Array) {
			element = shape.Elem()
		}
		for decoder.More() {
			if err := jsonValue(decoder, depth+1, element); err != nil {
				return err
			}
		}
	default:
		return errInvalid
	}
	_, err = decoder.Token()
	return err
}

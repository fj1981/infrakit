package cyutil

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cast"
)

func ToJson[T any](d T) (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func FromJson[T any](b string) (*T, error) {
	var v T
	err := json.Unmarshal([]byte(b), &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func StructToMap[T any](o *T, tag ...string) (map[string]interface{}, error) {
	var r map[string]interface{}
	dTag := "json"
	if len(tag) > 0 {
		dTag = tag[0]
	}
	forceOmitempty := false
	for _, v := range tag {
		if v == "omitempty" {
			forceOmitempty = true
		}
	}
	dc, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		TagName:          dTag,
		Result:           &r,
		DecodeHook: func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
			return filterEmptyFields(data, dTag, forceOmitempty), nil
		},
	})
	if err != nil {
		return nil, err
	}
	err = dc.Decode(o)
	return r, err
}

func MyStringToTimeHook() mapstructure.DecodeHookFunc {
	return func(f, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String || t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}
		return time.Parse("2006-01-02 15:04:05", data.(string))
	}
}

func MapToStruct[T any](m map[string]interface{}, tag ...string) (*T, error) {
	var r T
	dTag := "json"
	if len(tag) > 0 {
		dTag = tag[0]
	}
	dc, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:       MyStringToTimeHook(),
		WeaklyTypedInput: true,
		TagName:          dTag,
		Result:           &r,
	})
	if err != nil {
		return nil, err
	}
	err = dc.Decode(m)
	return &r, err
}
func SliceMapToSliceStruct[T any](m []map[string]interface{}, tag ...string) ([]*T, error) {
	var r []*T
	for _, v := range m {
		t, err := MapToStruct[T](v, tag...)
		if err != nil {
			return nil, err
		}
		r = append(r, t)
	}
	return r, nil
}

// filterEmptyFields 过滤结构体中的空字段
func filterEmptyFields(data interface{}, tagName string, forceOmitempty bool) interface{} {
	if data == nil {
		return nil
	}

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return data
	}

	result := make(map[string]interface{})
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// 跳过未导出的字段
		if !field.CanInterface() {
			continue
		}

		// 获取字段名（优先使用tag）
		fieldName := getFieldName(fieldType, tagName)
		if fieldName == "-" {
			continue
		}
		if isEmptyValue(field) {
			if forceOmitempty || hasOmitEmpty(fieldType, tagName) {
				continue
			}
		}
		result[fieldName] = field.Interface()
	}
	return result
}

// getFieldName 获取字段名，优先使用tag
func getFieldName(field reflect.StructField, tagName string) string {
	tag := field.Tag.Get(tagName)
	if tag != "" {
		// 处理 json:",omitempty" 这种情况
		parts := strings.Split(tag, ",")
		if parts[0] != "" {
			return parts[0]
		}
	}
	return field.Name
}

// hasOmitEmpty 检查字段是否有omitempty标签
func hasOmitEmpty(field reflect.StructField, tagName string) bool {
	tag := field.Tag.Get(tagName)
	if tag == "" {
		return false
	}
	// 检查是否包含omitempty
	parts := strings.Split(tag, ",")
	for _, part := range parts[1:] { // 跳过第一个部分（字段名）
		if strings.TrimSpace(part) == "omitempty" {
			return true
		}
	}
	return false
}

// isEmptyValue 判断反射值是否为空
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	default:
		return false
	}
}

func ToStr(value interface{}) string {
	if value == nil {
		return ""
	}
	if reflect.TypeOf(value).Kind() == reflect.Map ||
		reflect.TypeOf(value).Kind() == reflect.Slice {
		r, _ := ToJson(value)
		return r
	}
	v, err := cast.ToStringE(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return v
}

var ToString = ToStr

func ToInt(value interface{}) int {
	v, err := cast.ToIntE(value)
	if err != nil {
		return 0
	}
	return v
}

func ToInt64(value interface{}) int64 {
	v, err := cast.ToInt64E(value)
	if err != nil {
		return 0
	}
	return v
}

func ToFloat64(value interface{}) float64 {
	v, err := cast.ToFloat64E(value)
	if err != nil {
		return 0
	}
	return v
}

func ToBool(value interface{}) bool {
	v, err := cast.ToBoolE(value)
	if err != nil {
		return false
	}
	return v
}

func Ptr[T any](v T) *T {
	return &v
}

func IsDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func GetStr(m map[string]interface{}, key string, igoreCase ...bool) string {
	if m == nil {
		return ""
	}
	keys := strings.Split(key, ".")
	v, _ := GetValue(m, keys, igoreCase...)
	return ToStr(v)
}

func GetInt(m map[string]interface{}, key string, igoreCase ...bool) int {
	if m == nil {
		return 0
	}
	keys := strings.Split(key, ".")
	v, _ := GetValue(m, keys, igoreCase...)
	if v == nil {
		return 0
	}
	return ToInt(v)
}

func GetInt64(m map[string]interface{}, key string, igoreCase ...bool) int64 {
	if m == nil {
		return 0
	}
	keys := strings.Split(key, ".")
	v, _ := GetValue(m, keys, igoreCase...)
	if v == nil {
		return 0
	}
	return ToInt64(v)
}

func GetFloat(m map[string]interface{}, key string, igoreCase ...bool) float64 {
	if m == nil {
		return 0
	}
	keys := strings.Split(key, ".")
	v, _ := GetValue(m, keys, igoreCase...)
	if v == nil {
		return 0
	}
	return ToFloat64(v)
}

func GetBool(m map[string]interface{}, key string, igoreCase ...bool) bool {
	if m == nil {
		return false
	}
	keys := strings.Split(key, ".")
	v, _ := GetValue(m, keys, igoreCase...)
	if v == nil {
		return false
	}
	return cast.ToBool(v)
}

func GetVal[T any](m map[string]interface{}, key string, igoreCase ...bool) T {
	if m == nil {
		return *new(T)
	}
	keys := strings.Split(key, ".")
	v, err := GetValue(m, keys, igoreCase...)
	if err != nil {
		return *new(T)
	}
	if v == nil {
		return *new(T)
	}
	return v.(T)
}

func SliceToAny[T any](s []T) []any {
	result := make([]any, len(s))
	for i, v := range s {
		result[i] = v
	}
	return result
}

func MergeMaps(m1 ...map[string]interface{}) map[string]interface{} {
	if len(m1) == 0 {
		return nil
	}
	if len(m1) == 1 {
		return m1[0]
	}
	result := make(map[string]interface{})
	for _, m := range m1 {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func NewObj[T any]() (T, bool) {
	var ptr T
	t := reflect.TypeOf(ptr)
	if t.Kind() != reflect.Ptr {
		return ptr, false
	}
	elemType := t.Elem()
	newValue := reflect.New(elemType).Interface()
	return newValue.(T), true
}

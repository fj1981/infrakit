// dynaswag/schema.go
package cyswag

import (
	"reflect"
	"strconv"
	"strings"
)

func buildSchema(i interface{}) map[string]interface{} {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	schema := map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
	}

	var required []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}
		name := strings.Split(jsonTag, ",")[0]

		prop := map[string]interface{}{
			"type":        goTypeToSwagger(field.Type),
			"description": field.Tag.Get("description"),
			"example":     field.Tag.Get("example"),
			"format":      field.Tag.Get("format"),
		}

		if min := field.Tag.Get("minimum"); min != "" {
			if n, err := strconv.ParseFloat(min, 64); err == nil {
				prop["minimum"] = n
			}
		}
		if max := field.Tag.Get("maximum"); max != "" {
			if n, err := strconv.ParseFloat(max, 64); err == nil {
				prop["maximum"] = n
			}
		}

		if ml := field.Tag.Get("minLength"); ml != "" {
			if n, err := strconv.Atoi(ml); err == nil {
				prop["minLength"] = n
			}
		}
		if ml := field.Tag.Get("maxLength"); ml != "" {
			if n, err := strconv.Atoi(ml); err == nil {
				prop["maxLength"] = n
			}
		}

		if enum := field.Tag.Get("enum"); enum != "" {
			prop["enum"] = strings.Split(enum, ",")
		}

		schema["properties"].(map[string]interface{})[name] = prop

		if !strings.Contains(jsonTag, "omitempty") {
			required = append(required, name)
		}
	}

	if len(required) > 0 {
		schema["required"] = required
	}

	return schema
}

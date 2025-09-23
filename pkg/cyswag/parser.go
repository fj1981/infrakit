package cyswag

import (
	"mime/multipart"
	"reflect"
	"strings"
)

type Parameter struct {
	Name        string      `json:"name"`
	In          string      `json:"in"`
	Description string      `json:"description,omitempty"`
	Required    bool        `json:"required"`
	Type        string      `json:"type,omitempty"`
	Format      string      `json:"format,omitempty"`
	Schema      interface{} `json:"schema,omitempty"`
	Enum        []string    `json:"enum,omitempty"`
}

func goTypeToSwagger(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	default:
		return "string"
	}
}

// parseStructFields recursively parses struct fields including embedded structs
func parseStructFields(t reflect.Type) (hasJsonBody bool, hasFormData bool) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// If this is an embedded struct, recursively check its fields
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			hasJson, hasForm := parseStructFields(field.Type)
			hasJsonBody = hasJsonBody || hasJson
			hasFormData = hasFormData || hasForm
			continue
		}

		// 检查是否有JSON标签，且没有header标签
		if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" && field.Tag.Get("header") == "" {
			hasJsonBody = true
		}

		// 检查是否有表单数据
		if formTag := field.Tag.Get("form"); formTag != "" {
			hasFormData = true
		}
	}
	return hasJsonBody, hasFormData
}

type StructParams struct {
	Params     []Parameter
	StructName string
	Schema     map[string]interface{}
}

func ParseParameters(req any) *StructParams {
	if req == nil {
		return nil
	}
	if r, ok := req.(*StructParams); ok {
		return r
	}
	if r, ok := req.(StructParams); ok {
		return &r
	}
	var params []Parameter
	t := reflect.TypeOf(req)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 检查是否有JSON请求体
	hasJsonBody, hasFormData := parseStructFields(t)

	// 如果同时有JSON请求体和表单数据，优先处理表单数据
	// 因为在Swagger中，一个操作只能有一种请求体类型

	// 如果有JSON请求体且没有表单数据，添加一个body参数
	if hasJsonBody && !hasFormData {
		// 创建一个新的Schema对象，只包含JSON字段
		// 这样可以确保生成的请求体中不包含非JSON字段

		// 为请求体添加一个参数
		param := Parameter{
			Name:        "body",
			In:          "body",
			Description: "请求体参数",
			Required:    true,
			Schema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		}

		// 添加JSON字段到请求体Schema中
		properties := param.Schema.(map[string]interface{})["properties"].(map[string]interface{})
		requiredFields := []string{}

		// 递归处理JSON字段，包括嵌套结构体
		processJsonFields(t, properties, &requiredFields)

		// 添加必填字段
		if len(requiredFields) > 0 {
			param.Schema.(map[string]interface{})["required"] = requiredFields
		}

		params = append(params, param)
	}

	// 处理其他类型的参数（路径、查询、头部、表单）
	processNonJsonParams(t, &params, hasJsonBody, hasFormData)

	return &StructParams{
		Params:     params,
		StructName: getStructName(req),
		Schema:     buildSchema(req),
	}
}

// processJsonFields 递归处理JSON字段，包括嵌套结构体
func processJsonFields(t reflect.Type, properties map[string]interface{}, requiredFields *[]string) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 处理嵌套结构体
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			processJsonFields(field.Type, properties, requiredFields)
			continue
		}

		// 只处理有JSON标签且没有header标签的字段
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" || field.Tag.Get("header") != "" || field.Tag.Get("uri") != "" || field.Tag.Get("query") != "" {
			continue
		}

		// 获取JSON字段名称
		jsonName := strings.Split(jsonTag, ",")[0]

		// 添加字段到属性中
		prop := map[string]interface{}{
			"type": goTypeToSwagger(field.Type),
		}

		// 添加描述
		comment := strings.TrimSpace(field.Tag.Get("comment"))
		if comment == "" {
			comment = strings.TrimSpace(field.Tag.Get("//"))
		}
		if comment != "" {
			prop["description"] = comment
		}

		// 添加示例
		example := field.Tag.Get("example")
		if example != "" {
			prop["example"] = example
		}

		// 检查是否必填
		bindingTag := field.Tag.Get("binding")
		if bindingTag != "" && strings.Contains(bindingTag, "required") {
			*requiredFields = append(*requiredFields, jsonName)
		}

		// 处理枚举值
		if bindingTag != "" && strings.Contains(bindingTag, "oneof=") {
			for _, part := range strings.Split(bindingTag, ",") {
				if strings.HasPrefix(part, "oneof=") {
					enumValues := strings.TrimPrefix(part, "oneof=")
					prop["enum"] = strings.Split(enumValues, " ")
					break
				}
			}
		}

		properties[jsonName] = prop
	}
}

// processNonJsonParams 递归处理非JSON参数（路径、查询、头部、表单）
func processNonJsonParams(t reflect.Type, params *[]Parameter, hasJsonBody, hasFormData bool) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 处理嵌套结构体
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			processNonJsonParams(field.Type, params, hasJsonBody, hasFormData)
			continue
		}

		// Skip fields marked with swaggerignore
		if field.Tag.Get("swaggerignore") == "true" {
			continue
		}

		// 如果有JSON请求体且没有表单数据，跳过JSON字段
		// 但如果字段同时有header标签，则不跳过，因为它将被处理为请求头参数
		if hasJsonBody && !hasFormData {
			if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" && field.Tag.Get("header") == "" {
				continue
			}
		}

		// Check for different parameter types
		var paramName string
		var in string
		var required bool
		var description string
		var example string

		// 标签检查的优先级顺序： uri > header > query > form > json

		// 1. Path parameters (uri tag) - 最高优先级
		if uriTag := field.Tag.Get("uri"); uriTag != "" {
			paramName = uriTag
			in = "path"
			required = true // Path parameters are always required
		} else if headerTag := field.Tag.Get("header"); headerTag != "" {
			// 2. Header parameters - 次高优先级
			paramName = headerTag
			in = "header"
		} else if queryTag := field.Tag.Get("query"); queryTag != "" {
			// 3. Query parameters (query tag)
			paramName = queryTag
			in = "query"
		} else if formTag := field.Tag.Get("form"); formTag != "" {
			// 4. Form data (form tag)
			paramName = formTag
			in = "formData"
		}

		// 向后兼容：检查binding标签中的参数类型
		if bindingTag := field.Tag.Get("binding"); bindingTag != "" {
			// 如果有form标签但没有明确指定类型，检查binding中是否有指定
			if paramName != "" && in == "formData" {
				if strings.Contains(bindingTag, "query") {
					in = "query" // 如果binding中包含query，则覆盖为查询参数
				}
			}
		}

		// Skip if no valid parameter type was found
		if paramName == "" {
			continue
		}

		// Parse binding tag for required validation
		if bindingTag := field.Tag.Get("binding"); bindingTag != "" {
			if strings.Contains(bindingTag, "required") {
				required = true
			}
		}

		// Get description from comments (using the field comment)
		comment := strings.TrimSpace(field.Tag.Get("comment"))
		if comment == "" {
			// Try to get description from the field comment in the struct
			comment = strings.TrimSpace(field.Tag.Get("//"))
		}
		description = comment

		// Get example value
		example = field.Tag.Get("example")

		param := Parameter{
			Name:        paramName,
			In:          in,
			Description: description,
			Required:    required,
		}

		// Handle file uploads
		if in == "formData" && field.Type == reflect.TypeOf((*multipart.FileHeader)(nil)) {
			param.Type = "file"
		} else {
			param.Type = goTypeToSwagger(field.Type)

			// Handle enum values from binding tag
			if bindingTag := field.Tag.Get("binding"); bindingTag != "" && strings.Contains(bindingTag, "oneof=") {
				for _, part := range strings.Split(bindingTag, ",") {
					if strings.HasPrefix(part, "oneof=") {
						enumValues := strings.TrimPrefix(part, "oneof=")
						param.Enum = strings.Split(enumValues, " ")
						break
					}
				}
			}
		}

		// Add example if available
		if example != "" {
			// In Swagger 2.0, example is not directly supported at parameter level
			// We could add it as part of the description
			if param.Description != "" {
				param.Description += " (Example: " + example + ")"
			} else {
				param.Description = "Example: " + example
			}
		}

		*params = append(*params, param)
	}
}

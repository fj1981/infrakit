package cyswag

import (
	"encoding/json"
	"reflect"
	"strings"
)

type APIEndpoint struct {
	Path        string
	Method      string
	Summary     string
	Description string
	Tags        []string
	Request     any
	Response    any
}

type Generator struct {
	title     string
	version   string
	host      string
	basePath  string
	schemes   []string
	endpoints []APIEndpoint
}

func NewGenerator(title, version string) *Generator {
	return &Generator{
		title:     title,
		version:   version,
		basePath:  "/",
		schemes:   []string{"http"},
		endpoints: []APIEndpoint{},
	}
}

func (g *Generator) Host(host string) *Generator {
	g.host = host
	return g
}

func (g *Generator) BasePath(path string) *Generator {
	g.basePath = path
	return g
}

func (g *Generator) Scheme(s ...string) *Generator {
	g.schemes = s
	return g
}

func (g *Generator) AddAPI(api APIEndpoint) *Generator {
	g.endpoints = append(g.endpoints, api)
	return g
}

func (g *Generator) GenerateTemplate() string {
	swagger := map[string]interface{}{
		"swagger": "2.0",
		"info": map[string]string{
			"title":   g.title,
			"version": g.version,
		},
		"host":        g.host,
		"basePath":    g.basePath,
		"schemes":     g.schemes,
		"paths":       map[string]interface{}{},
		"definitions": map[string]interface{}{},
	}
	/*
		typeNames := map[string]bool{}
		for _, api := range g.endpoints {
			if api.Request != nil {
				typeName := getStructName(api.Request)
				if !typeNames[typeName] {
					swagger["definitions"].(map[string]interface{})[typeName] = g.buildSchema(api.Request)
					typeNames[typeName] = true
				}
			}
			if api.Response != nil {
				typeName := getStructName(api.Response)
				if !typeNames[typeName] {
					swagger["definitions"].(map[string]interface{})[typeName] = g.buildSchema(api.Response)
					typeNames[typeName] = true
				}
			}
		}
	*/
	typeNames := map[string]bool{"": true}
	for _, api := range g.endpoints {
		// 只使用 api.Path，不要再添加 basePath，因为 basePath 已经在 Swagger 规范中定义
		fullPath := g.ensureLeadingSlash(api.Path)
		if _, exists := swagger["paths"].(map[string]interface{})[fullPath]; !exists {
			swagger["paths"].(map[string]interface{})[fullPath] = map[string]interface{}{}
		}
		pathItem := swagger["paths"].(map[string]interface{})[fullPath].(map[string]interface{})

		op := map[string]interface{}{
			"tags":        api.Tags,
			"summary":     api.Summary,
			"description": api.Description,
			"parameters":  []interface{}{},
		}

		hasFile := false
		if api.Request != nil {
			structParams := ParseParameters(api.Request)
			if structParams != nil {
				for _, p := range structParams.Params {
					op["parameters"] = append(op["parameters"].([]interface{}), p)
					if p.In == "formData" || p.Type == "file" {
						hasFile = true
					}
				}

				if len(op["parameters"].([]interface{})) > 0 {
					if hasFile {
						op["consumes"] = []string{"multipart/form-data"}
					} else {
						op["consumes"] = []string{"application/json"}
					}
				}

				typeName := structParams.StructName
				if !typeNames[typeName] {
					swagger["definitions"].(map[string]interface{})[typeName] = structParams.Schema
					typeNames[typeName] = true
				}
				if api.Response != nil {
					typeName := getStructName(api.Response)
					if !typeNames[typeName] {
						b := buildSchema(api.Response)
						if b != nil {
							swagger["definitions"].(map[string]interface{})[typeName] = b
							typeNames[typeName] = true
						}
					}
				}
			}
		}

		op["responses"] = map[string]interface{}{
			"200": map[string]interface{}{
				"description": "OK",
			},
		}

		if api.Response != nil {
			op["responses"].(map[string]interface{})["200"].(map[string]interface{})["schema"] = map[string]string{
				"$ref": "#/definitions/" + getStructName(api.Response),
			}
		}

		method := strings.ToLower(api.Method)
		pathItem[method] = op
		swagger["paths"].(map[string]interface{})[fullPath] = pathItem
	}

	data, _ := json.MarshalIndent(swagger, "", "  ")
	return string(data)
}

func (g *Generator) ensureLeadingSlash(path string) string {
	if path == "" || path[0] == '/' {
		return path
	}
	return "/" + path
}

func getStructName(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

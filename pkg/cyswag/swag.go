package cyswag

import (
	"github.com/swaggo/swag"
)

type RegisterOption func(*Generator)

func WithTitle(title string) RegisterOption {
	return func(g *Generator) {
		g.title = title
	}
}
func WithVersion(version string) RegisterOption {
	return func(g *Generator) {
		g.version = version
	}
}
func WithHost(host string) RegisterOption {
	return func(g *Generator) {
		g.host = host
	}
}
func WithBasePath(basePath string) RegisterOption {
	return func(g *Generator) {
		g.basePath = basePath
	}
}
func WithScheme(schemes ...string) RegisterOption {
	return func(g *Generator) {
		g.schemes = schemes
	}
}
func WithApiEndpoint(api ...APIEndpoint) RegisterOption {
	return func(g *Generator) {
		g.endpoints = append(g.endpoints, api...)
	}
}

func Register(opts ...RegisterOption) {
	generator := NewGenerator("系统 API", "1.0.0")
	for _, opt := range opts {
		opt(generator)
	}
	SwaggerInfo := &swag.Spec{
		Version:          generator.version,
		Host:             generator.host,
		BasePath:         generator.basePath,
		Schemes:          generator.schemes,
		Title:            generator.title,
		Description:      "API服务",
		InfoInstanceName: "swagger",
		SwaggerTemplate:  generator.GenerateTemplate(),
	}

	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}

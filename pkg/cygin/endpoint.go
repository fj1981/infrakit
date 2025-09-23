package cygin

import "github.com/gin-gonic/gin"

// EndpointBuilder 是一个用于构建 API 端点的辅助函数集合
type EndpointBuilder struct {
	basePath    string
	description string
	tags        []string
}

// EndpointOption 用于配置端点的可选参数
type EndpointOption func(*ApiEndpoint)

// NewEndpointBuilder 创建一个新的端点构建器
func NewEndpointBuilder(basePath string, description string, tags []string) *EndpointBuilder {
	return &EndpointBuilder{
		basePath:    basePath,
		description: description,
		tags:        tags,
	}
}

// WithMiddleware 设置端点中间件
func WithMiddleware(middleware ...gin.HandlerFunc) EndpointOption {
	return func(e *ApiEndpoint) {
		e.Middleware = middleware
	}
}

type GroupOption func(*ApiGroup)

func WithGroupDescription(description string) GroupOption {
	return func(e *ApiGroup) {
		e.Description = description
	}
}

func WithGroupTags(tags ...string) GroupOption {
	return func(e *ApiGroup) {
		e.Tags = tags
	}
}

func WithGroupMiddleware(middleware ...gin.HandlerFunc) GroupOption {
	return func(e *ApiGroup) {
		e.Middleware = middleware
	}
}

// Build 构建一个API组
func (b *EndpointBuilder) Build(apiHandlers ...APIHandler) ApiGroup {
	return ApiGroup{
		BasePath:    b.basePath,
		Description: b.description,
		Tags:        b.tags,
		APIHandler:  apiHandlers,
	}
}

// WithSummary 设置端点摘要
func WithSummary(summary string) EndpointOption {
	return func(e *ApiEndpoint) {
		e.Summary = summary
	}
}

// WithDescription 设置端点描述
func WithDescription(description string) EndpointOption {
	return func(e *ApiEndpoint) {
		e.Description = description
	}
}

// WithTags 设置端点标签
func WithTags(tags ...string) EndpointOption {
	return func(e *ApiEndpoint) {
		e.Tags = tags
	}
}

// applyOptions 应用所有选项到端点
func applyOptions(endpoint *ApiEndpoint, options ...EndpointOption) {
	for _, option := range options {
		option(endpoint)
	}
}

// Endpoint 创建一个基础端点
type Endpoint struct {
	builder *EndpointBuilder
	path    string
	method  ApiMethod
	handler interface{}
}

// WithSummary 设置端点摘要
func (e Endpoint) WithSummary(summary string) Endpoint {
	e.handler = WithSummary(summary)
	return e
}

// WithDescription 设置端点描述
func (e Endpoint) WithDescription(description string) Endpoint {
	e.handler = WithDescription(description)
	return e
}

// WithTags 设置端点标签
func (e Endpoint) WithTags(tags ...string) Endpoint {
	e.handler = WithTags(tags...)
	return e
}

// Build 构建最终的 ApiEndpoint
func (e Endpoint) Build() ApiEndpoint {
	endpoint := ApiEndpoint{
		Path:    e.path,
		Method:  e.method,
		Handler: e.handler,
	}

	// 应用默认标签
	if len(endpoint.Tags) == 0 && len(e.builder.tags) > 0 {
		endpoint.Tags = e.builder.tags
	}

	return endpoint
}

// GET 创建一个GET方法的端点
func (b *EndpointBuilder) GET(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Get,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// POST 创建一个POST方法的端点
func (b *EndpointBuilder) POST(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Post,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// PUT 创建一个PUT方法的端点
func (b *EndpointBuilder) PUT(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Put,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// DELETE 创建一个DELETE方法的端点
func (b *EndpointBuilder) DELETE(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Delete,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// PATCH 创建一个PATCH方法的端点
func (b *EndpointBuilder) PATCH(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Patch,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// OPTIONS 创建一个OPTIONS方法的端点
func (b *EndpointBuilder) OPTIONS(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Options,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

// HEAD 创建一个HEAD方法的端点
func (b *EndpointBuilder) HEAD(path string, handler interface{}, options ...EndpointOption) APIHandler {
	// 初始化端点，不设置默认标签
	endpoint := ApiEndpoint{
		Path:    path,
		Method:  Head,
		Handler: handler,
	}

	// 应用选项
	applyOptions(&endpoint, options...)

	// 如果没有设置标签，使用默认标签
	if len(endpoint.Tags) == 0 {
		endpoint.Tags = b.tags
	}

	return &endpoint
}

func (b *EndpointBuilder) GROUP(path string, handlers []APIHandler, options ...GroupOption) APIHandler {
	ag := &ApiGroup{
		BasePath:   path,
		APIHandler: handlers,
	}
	for _, opf := range options {
		opf(ag)
	}
	return ag
}

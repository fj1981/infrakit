package cygin

import (
	"fmt"
	"strings"

	"github.com/fj1981/infrakit/pkg/cyswag"
	"github.com/gin-gonic/gin"
)

type ApiMethod string

const (
	Get     ApiMethod = "GET"
	Post    ApiMethod = "POST"
	Put     ApiMethod = "PUT"
	Delete  ApiMethod = "DELETE"
	Patch   ApiMethod = "PATCH"
	Options ApiMethod = "OPTIONS"
	Head    ApiMethod = "HEAD"
	Any     ApiMethod = "ANY"
)

type RegistResult struct {
	apiEndpoints []cyswag.APIEndpoint
	groupPath    map[string]struct{}
}

type APIHandler interface {
	RegistRouter(basePath string, g *gin.RouterGroup) *RegistResult
}

type ApiEndpoint struct {
	Path        string
	Method      ApiMethod
	Summary     string
	Description string
	Tags        []string
	Handler     any
	Middleware  []gin.HandlerFunc
}

func (r *RegistResult) Merge(other *RegistResult) *RegistResult {
	if r.groupPath == nil {
		r.groupPath = make(map[string]struct{})
	}
	for k := range other.groupPath {
		r.groupPath[k] = struct{}{}
	}
	r.apiEndpoints = append(r.apiEndpoints, other.apiEndpoints...)
	return r
}

func (ae *ApiEndpoint) RegistRouter(basePath string, g *gin.RouterGroup) *RegistResult {
	path := pathJoin(basePath, convertGinPathToSwaggerPath(ae.Path))

	apiEndpoint := cyswag.APIEndpoint{
		Path:        path,
		Method:      string(ae.Method),
		Summary:     ae.Summary,
		Description: ae.Description,
		Tags:        ae.Tags,
	}
	ff, err := HandleAny(ae.Handler, func(input any, out any) {
		apiEndpoint.Request = input
		apiEndpoint.Response = out
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to register endpoint%v, error: %v", ae, err.Error()))
	}
	switch ae.Method {
	case Get:
		g.GET(ae.Path, append(ae.Middleware, ff)...)
	case Post:
		g.POST(ae.Path, append(ae.Middleware, ff)...)
	case Put:
		g.PUT(ae.Path, append(ae.Middleware, ff)...)
	case Delete:
		g.DELETE(ae.Path, append(ae.Middleware, ff)...)
	case Patch:
		g.PATCH(ae.Path, append(ae.Middleware, ff)...)
	case Options:
		g.OPTIONS(ae.Path, append(ae.Middleware, ff)...)
	case Head:
		g.HEAD(ae.Path, append(ae.Middleware, ff)...)
	case Any:
		g.Any(ae.Path, append(ae.Middleware, ff)...)
	}
	return &RegistResult{
		apiEndpoints: []cyswag.APIEndpoint{apiEndpoint},
		groupPath:    map[string]struct{}{basePath: {}},
	}
}

type ApiGroup struct {
	BasePath    string
	Description string
	Tags        []string
	Middleware  []gin.HandlerFunc
	APIHandler  []APIHandler
}

func pathJoin(basePath string, path string) string {
	basePath = strings.TrimSpace(basePath)
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/") {
		return basePath + "/" + path
	}
	return basePath + path
}

func (ag *ApiGroup) RegistRouter(basePath string, g *gin.RouterGroup) *RegistResult {
	r := &RegistResult{}
	rg := g.Group(ag.BasePath, ag.Middleware...)
	basePath = pathJoin(basePath, ag.BasePath)
	for _, handler := range ag.APIHandler {
		r.Merge(handler.RegistRouter(basePath, rg))
	}
	return r
}

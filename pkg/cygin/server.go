package cygin

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/fj1981/infrakit/pkg/cyfct"
	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyswag"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// ==================== 全局变量（用于注入版本）====================
var (
	BuildVersion = "dev"
	BuildTime    = "unknown"
)

type StaticFileConfig struct {
	UrlPath string
	DirPath string
}

type EmbeddedFileConfig struct {
	UrlPath string
	FS      embed.FS
	Root    string
}

type Config struct {
	Address       string
	Env           string
	EnablePprof   bool
	EnableSwagger bool
	Title         string
	Version       string
	Description   string
	AutoRegister  bool
	staticFiles   []StaticFileConfig
	embeddedFiles []EmbeddedFileConfig
	basePaths     []string
}

type RouterGroup interface {
	Group() ApiGroup
}

func loadConfig() *Config {
	return &Config{
		Address:     getEnv("SERVER_ADDRESS", ":8080"),
		Env:         getEnv("ENV", "prod"),
		EnablePprof: getEnv("ENABLE_PPROF", "") != "", // 开启 pprof 性能分析
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// ==================== Server 结构体 ====================
type Server struct {
	Engine *gin.Engine
	Config *Config
	// PathPrefixFilter 存储需要保护的API路径前缀
	PathPrefixFilter map[string]bool
	ApiGroups        []ApiGroup
	swaggerOption    []cyswag.RegisterOption
}

type ServerOption func(*Server)

// ==================== Gin Mode 控制 ====================
func WithMode(mode string) ServerOption {
	return func(s *Server) {
		gin.SetMode(mode)
	}
}

// ==================== 中间件与功能 ====================

// WithCORS 启用跨域
func WithCORS(allowOrigins ...string) ServerOption {
	return func(s *Server) {
		config := cors.DefaultConfig()
		if len(allowOrigins) == 0 {
			config.AllowOrigins = []string{
				"http://localhost:3000",
				"http://127.0.0.1:3000",
				"http://localhost:5173",
			}
		} else {
			config.AllowOrigins = allowOrigins
		}
		config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"}
		config.AllowHeaders = []string{"Origin", "Content-Type", "Authorization", "Accept"}
		config.AllowCredentials = true
		s.Engine.Use(cors.New(config))
	}
}

// WithSwagger 启用 Swagger 文档
func WithSwagger(swaggerOption ...cyswag.RegisterOption) ServerOption {
	return func(s *Server) {
		s.Config.EnableSwagger = true
		s.swaggerOption = swaggerOption
	}
}

// WithHealthCheck 健康检查
func WithHealthCheck() ServerOption {
	return func(s *Server) {
		s.Engine.GET("/healthz", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"status": "ok",
				"time":   time.Now().UTC().Format(time.RFC3339),
			})
		})
		s.Engine.GET("/ping", func(c *gin.Context) {
			c.String(200, "pong")
		})
	}
}

func WithBasePath(basePath ...string) ServerOption {
	return func(s *Server) {
		s.Config.basePaths = append(s.Config.basePaths, basePath...)
	}
}

// WithVersionInfo 版本信息
func WithVersionInfo() ServerOption {
	return func(s *Server) {
		s.Engine.GET("/version", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"version": BuildVersion,
				"build":   BuildTime,
				"env":     s.Config.Env,
				"mode":    gin.Mode(),
			})
		})
	}
}

// AddPathPrefix 添加需要保护的API路径前缀
// 当使用WithEmbeddedFiles注册根路径时，这些前缀将不会被静态文件处理器处理
func (s *Server) AddPathPrefix(prefix string) {
	if s.PathPrefixFilter == nil {
		s.PathPrefixFilter = make(map[string]bool)
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	s.PathPrefixFilter[prefix] = true
}

// WithAutoRegister 自动注册路由
func WithAutoRegister() ServerOption {
	return func(s *Server) {
		s.Config.AutoRegister = true
	}
}

// WithPProf 启用 pprof 性能分析（仅开发环境建议开启）
func WithPProf() ServerOption {
	return func(s *Server) {
		pprof.Register(s.Engine)
	}
}

func AddApiGroup(group ...ApiGroup) ServerOption {
	return func(s *Server) {
		s.ApiGroups = append(s.ApiGroups, group...)
		for _, g := range group {
			basePath := g.BasePath
			if !strings.HasPrefix(basePath, "/") {
				basePath = "/" + basePath
			}
			s.PathPrefixFilter[basePath] = true
		}
	}
}

// AddRouteGroup 添加路由分组
func AddRouteGroup(prefix string, routes func(*gin.RouterGroup)) ServerOption {
	return func(s *Server) {
		// 创建路由组
		group := s.Engine.Group(prefix)
		routes(group)

		// 自动将路由组前缀添加到路径过滤器中
		// 这样当使用WithEmbeddedFiles注册根路径时，这些前缀将不会被静态文件处理器处理
		if s.PathPrefixFilter == nil {
			s.PathPrefixFilter = make(map[string]bool)
		}

		// 添加路由组前缀
		// 如果前缀不以"/"开头，添加"/"
		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}
		s.PathPrefixFilter[prefix] = true
	}
}

// WithPort 设置端口
func WithPort(port int) ServerOption {
	return func(s *Server) {
		s.Config.Address = fmt.Sprintf(":%d", port)
	}
}

// WithStaticFiles 提供静态文件服务
func WithStaticFiles(urlPath, dirPath string) ServerOption {
	return func(s *Server) {
		s.Config.staticFiles = append(s.Config.staticFiles, StaticFileConfig{
			UrlPath: urlPath,
			DirPath: dirPath,
		})
	}
}

// WithEmbeddedFiles 提供内嵌静态文件服务
// 使用方法：
// //go:embed assets/*
// var assets embed.FS
// server := cygin.NewServer(cygin.WithEmbeddedFiles("/assets", assets, "assets"))
func WithEmbeddedFiles(urlPath string, embeddedFS embed.FS, fsRoot string) ServerOption {
	return func(s *Server) {
		s.Config.embeddedFiles = append(s.Config.embeddedFiles, EmbeddedFileConfig{
			UrlPath: urlPath,
			FS:      embeddedFS,
			Root:    fsRoot,
		})
	}
}

func doSetupValidator() {
	once.Do(func() {
		SetupValidator()
	})
}

// convertGinPathToSwaggerPath 将Gin路径格式(:param)转换为Swagger路径格式({param})
func convertGinPathToSwaggerPath(ginPath string) string {
	// 使用正则表达式将 :param 替换为 {param}
	re := regexp.MustCompile(`:([^/]+)`)
	return re.ReplaceAllString(ginPath, "{$1}")
}

// NewServer 创建 Server
func NewServer(opts ...ServerOption) *Server {
	doSetupValidator()
	config := loadConfig()

	// === 默认 Mode: Release ===
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		// 使用 cylog 记录访问日志，模仿 Gin 的调试日志格式

		// 状态码颜色
		statusColor := param.StatusCodeColor()
		resetColor := param.ResetColor()

		// 方法颜色
		methodColor := param.MethodColor()

		// 响应时间格式化
		latency := cyutil.FormatDuration(param.Latency)

		// 解析用户代理
		uaInfo := cyutil.ParseUserAgent(param.Request.UserAgent())

		// 构建日志消息，模仿 Gin 的调试日志格式
		logMsg := fmt.Sprintf("%s%3d%s|%s| %s%s%s %s  %s [%s] %s",
			statusColor, param.StatusCode, resetColor,
			cyutil.PadStart(latency, 10, " "),
			methodColor, param.Method, resetColor,
			param.Request.Proto,
			param.Path,
			uaInfo,
			param.ClientIP,
		)

		// 根据状态码和响应时间选择日志级别
		latencyMs := param.Latency.Milliseconds()
		if param.StatusCode >= 400 || latencyMs > 5000 {
			cylog.Skip(0).Warn(logMsg)
		} else { // 超过5秒的请求使用警告级别
			cylog.Skip(0).Info(logMsg)
		}

		return ""
	}))

	s := &Server{
		Engine:           engine,
		Config:           config,
		PathPrefixFilter: make(map[string]bool),
	}

	// 应用用户选项（可覆盖 Mode、添加功能）
	for _, opt := range opts {
		opt(s)
	}
	apiGroups := s.ApiGroups
	if s.Config.AutoRegister {
		routers, _ := cyfct.GetInstancesByGroup[RouterGroup]("router")
		for _, router := range routers {
			apiGroups = append(apiGroups, router.Group())
		}
	}

	basePaths := s.Config.basePaths
	if len(basePaths) == 0 {
		basePaths = []string{""}
	}
	r := &RegistResult{}
	for _, basePath := range basePaths {
		g := engine.Group(basePath)
		s.AddPathPrefix(basePath)
		for _, apiGroup := range apiGroups {
			r.Merge(apiGroup.RegistRouter(basePath, g))
		}
	}
	for gp := range r.groupPath {
		s.PathPrefixFilter[gp] = true
	}

	if config.EnableSwagger {
		cyswag.Register(append(s.swaggerOption, cyswag.WithApiEndpoint(r.apiEndpoints...))...)
		s.PathPrefixFilter["/swagger"] = true
		s.Engine.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	for _, embeddedFileConfig := range s.Config.embeddedFiles {
		s.Engine.Use(Serve(embeddedFileConfig.UrlPath, EmbedFolder(embeddedFileConfig.FS, embeddedFileConfig.Root), func(path string) bool {
			for prefix := range s.PathPrefixFilter {
				if strings.HasPrefix(path, prefix) {
					return true
				}
			}
			return false
		}))
		cylog.Info("Embedded file server enabled with SPA support", "urlPath", embeddedFileConfig.UrlPath, "fsRoot", embeddedFileConfig.Root)
	}

	for _, staticFileConfig := range s.Config.staticFiles {
		s.Engine.Static(staticFileConfig.UrlPath, staticFileConfig.DirPath)
		cylog.Info("Static file server enabled", "urlPath", staticFileConfig.UrlPath, "dirPath", staticFileConfig.DirPath)
	}

	// === 条件性启用功能 ===
	if config.EnablePprof {
		WithPProf()(s)
	}

	return s
}

// ==================== 启动与关闭 ====================
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.Config.Address,
		Handler: s.Engine,
	}

	go func() {
		cylog.Info("Server starting", "address", s.Config.Address, "mode", gin.Mode(), "env", s.Config.Env)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			cylog.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cylog.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return cylog.Error("Server failed to shutdown", "error", err)
	}
	cylog.Info("Server stopped gracefully")
	return nil
}

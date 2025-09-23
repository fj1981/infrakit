// cyconf/cyconf.go
package cyconf

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/viper"
)

// === 配置源定义 ===

type ConfigSource interface {
	// Load 加载配置到 viper
	// 返回：是否加载成功，错误
	Load(v *viper.Viper) (bool, error)
}

// FlagSource 从命令行 flag 读取配置路径
type FlagSource struct {
	FlagName string
	Default  string // 如果 flag 未设置，用这个默认路径
	Type     string
}

func (s *FlagSource) Load(v *viper.Viper) (bool, error) {
	var path string

	// 1. 首先尝试从环境变量获取（用于测试或兼容旧代码）
	envPath := os.Getenv("FLAG_" + strings.ToUpper(s.FlagName))
	if envPath != "" {
		path = envPath
	} else {
		// 2. 尝试从命令行参数获取
		// 创建一个独立的 FlagSet，避免与全局 flag 冲突
		flagSet := flag.NewFlagSet(s.FlagName, flag.ContinueOnError)

		// 隐藏错误输出，避免命令行参数不存在时的错误打印
		flagSet.SetOutput(io.Discard)

		// 定义配置路径参数
		configPath := flagSet.String(s.FlagName, s.Default, "Path to config file")

		// 尝试解析，忽略错误（因为可能参数不存在）
		// 使用单独的 FlagSet 避免影响全局 flag 状态
		_ = flagSet.Parse(os.Args[1:])

		// 获取解析结果
		path = *configPath
	}

	// 3. 如果上述方法均未设置路径，使用默认值
	if path == "" {
		path = s.Default
	}

	// 如果没有有效路径，返回未加载
	if path == "" {
		return false, nil
	}
	type_ := detectType(path)
	// 尝试加载配置文件
	loaded, err := loadFileIfExists(v, path, type_)
	if err != nil {
		return false, fmt.Errorf("failed to load config from flag %s (path: %s): %w", s.FlagName, path, err)
	}

	return loaded, nil
}

// EnvSource 从环境变量读取配置路径
type EnvSource struct {
	EnvVar string
	Type   string
}

func (s *EnvSource) Load(v *viper.Viper) (bool, error) {
	// 从环境变量获取配置路径
	path := os.Getenv(s.EnvVar)

	// 如果环境变量未设置，返回未加载
	if path == "" {
		return false, nil
	}

	if s.Type == "" {
		s.Type = detectType(path)
	}
	loaded, err := loadFileIfExists(v, path, s.Type)
	if err != nil {
		return false, fmt.Errorf("failed to load config from env %s (path: %s): %w", s.EnvVar, path, err)
	}

	return loaded, nil
}

// FileSource 指向一个配置文件（支持合并）
type FileSource struct {
	Path string
	Type string
}

func (s *FileSource) Load(v *viper.Viper) (bool, error) {
	if s.Type == "" {
		s.Type = detectType(s.Path)
	}
	loaded, err := loadFileIfExists(v, s.Path, s.Type)
	if err != nil {
		return false, fmt.Errorf("failed to load config from file %s: %w", s.Path, err)
	}

	return loaded, nil
}

// FileGroup 多个文件，支持合并
type FileGroup struct {
	Sources []FileSource
}

func (g *FileGroup) Load(v *viper.Viper) (bool, error) {
	var loaded bool
	var errs []error

	for _, src := range g.Sources {
		if src.Type == "" {
			src.Type = detectType(src.Path)
		}
		exists, err := loadSingleFile(v, src.Path, src.Type)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to load %s: %w", src.Path, err))
			continue // 尝试加载下一个文件
		}
		if exists {
			loaded = true
		}
	}

	if !loaded && len(errs) > 0 {
		return false, fmt.Errorf("failed to load any config file: %v", errs)
	}

	return loaded, nil
}

// === Option 定义 ===

type Option func(*configLoader)

type configLoader struct {
	flagSources []FlagSource
	envSources  []EnvSource
	fileGroup   *FileGroup // 支持多个文件合并

	// 默认回退
	defaultFlagName string
	defaultEnvVar   string
	defaultFilePath string

	// 验证错误
	err error
}

func newConfigLoader() *configLoader {
	return &configLoader{
		defaultFlagName: "config",
		defaultEnvVar:   "CONFIG_PATH",
		defaultFilePath: "config.yml",
	}
}

// WithFlag 从命令行 flag 读取配置路径
func WithFlag(flagName, defaultPath string) Option {
	return func(cl *configLoader) {

		cl.flagSources = append(cl.flagSources, FlagSource{
			FlagName: flagName,
			Default:  defaultPath,
		})
	}
}

// WithEnv 从环境变量读取配置路径
func WithEnv(envVar string) Option {
	return func(cl *configLoader) {
		cl.envSources = append(cl.envSources, EnvSource{EnvVar: envVar})
	}
}

// WithFile 添加一个配置文件（支持多个，自动合并）
// 如果类型不正确，将返回错误
func WithFile(path string) Option {
	return func(cl *configLoader) {
		if cl.fileGroup == nil {
			cl.fileGroup = &FileGroup{}
		}

		// 如果类型为空，自动检测
		detectedType := detectType(path)
		cl.fileGroup.Sources = append(cl.fileGroup.Sources, FileSource{Path: path, Type: detectedType})
	}
}

// WithFiles 添加多个配置文件（合并）
// 如果类型不正确，将返回错误
func WithFiles(paths ...string) Option {
	return func(cl *configLoader) {
		for _, path := range paths {
			WithFile(path)(cl)
			if cl.err != nil {
				return
			}
		}
	}
}

// WithDefaults 自定义默认值
func WithDefaults(flagName, envVar, filePath string) Option {
	return func(cl *configLoader) {
		cl.defaultFlagName = flagName
		cl.defaultEnvVar = envVar
		cl.defaultFilePath = filePath
	}
}

// === 主加载逻辑 ===

// configMu 保护配置加载过程的并发安全
var configMu sync.Mutex

func LoadConfig[T any](opts ...Option) (*T, error) {
	// 加锁确保并发安全
	configMu.Lock()
	defer configMu.Unlock()
	cl := newConfigLoader()

	// 应用用户选项
	for _, opt := range opts {
		opt(cl)
	}

	// 检查验证错误
	if cl.err != nil {
		return nil, fmt.Errorf("config validation error: %w", cl.err)
	}

	// 如果用户什么都没设置，使用默认三件套
	if len(cl.flagSources) == 0 && len(cl.envSources) == 0 && (cl.fileGroup == nil || len(cl.fileGroup.Sources) == 0) {
		WithFlag(cl.defaultFlagName, "")(cl)
		WithEnv(cl.defaultEnvVar)(cl)
		WithFile(cl.defaultFilePath)(cl)
	}

	v := viper.New()
	loaded := false
	var loadErr error

	// 🔝 1. 命令行优先
	for _, src := range cl.flagSources {
		if ok, err := src.Load(v); err == nil && ok {
			loaded = true
			break
		} else if err != nil {
			loadErr = err
		}
	}

	// 🔝 2. 环境变量
	if !loaded {
		for _, src := range cl.envSources {
			if ok, err := src.Load(v); err == nil && ok {
				loaded = true
				break
			} else if err != nil {
				loadErr = err
			}
		}
	}

	// 🔝 3. 配置文件（合并）
	if !loaded && cl.fileGroup != nil {
		if ok, err := cl.fileGroup.Load(v); err == nil && ok {
			loaded = true
		} else if err != nil {
			loadErr = err
		}
	}

	if !loaded {
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, fmt.Errorf("cyconf: no valid config found")
	}
	v.AutomaticEnv()
	var config T
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("cyconf: failed to unmarshal config: %w", err)
	}
	return &config, nil
}

// === 工具函数 ===

// detectType 从文件扩展名推断类型
func detectType(path string) string {
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".json":
		return "json"
	case ".toml":
		return "toml"
	case ".yaml", ".yml":
		return "yaml"
	default:
		return "yaml"
	}
}

func getFullPath(path string) string {
	// 如果路径为空，直接返回
	if path == "" {
		return path
	}

	// 如果路径已经是绝对路径（以/开头），直接返回
	if filepath.IsAbs(path) {
		return path
	}

	// 获取当前工作目录
	cwd, err := os.Getwd()
	if err != nil {
		// 如果获取当前目录失败，返回原路径
		return path
	}

	// 拼接完整路径
	return filepath.Join(cwd, path)
}

// loadFileIfExists 检查文件是否存在，存在则加载
// first: 是否是第一个加载的文件
func loadFileIfExists(v *viper.Viper, path, typ string) (bool, error) {
	path = getFullPath(path)
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return false, nil
	}
	return loadSingleFile(v, path, typ)
}

// loadSingleFile 加载单个文件
func loadSingleFile(v *viper.Viper, path, typ string) (bool, error) {
	if typ == "" {
		typ = detectType(path)
	}

	vp := viper.New()
	vp.SetConfigFile(path)
	vp.SetConfigType(typ)

	if err := vp.ReadInConfig(); err != nil {
		return false, err
	}

	// Always use Set instead of MergeConfigMap to ensure proper overriding of values
	// This ensures that nested maps are properly overridden
	settings := vp.AllSettings()
	v.MergeConfigMap(settings)
	return true, nil
}

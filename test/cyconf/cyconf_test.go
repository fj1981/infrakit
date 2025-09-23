package cyconf_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fj1981/infrakit/pkg/cyconf"
	"github.com/fj1981/infrakit/test/cyconf/testutil"
)

// 使用 testutil 包中的 TestConfig 和 CreateTestConfigFile

// 基本配置文件测试
func TestBasicConfigFile(t *testing.T) {
	// 创建临时配置文件
	configPath := filepath.Join(t.TempDir(), "config.yml")
	configContent := `
database:
  host: localhost
  port: 3306
  username: root
  password: password
server:
  port: 8080
  timeout: 30
features:
  logging: true
  metrics: false
`
	testutil.CreateTestConfigFile(t, configPath, configContent)

	// 使用 WithPath 加载配置
	t.Run("WithPath", func(t *testing.T) {
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithPath(configPath),
		)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "localhost" {
			t.Errorf("Expected Database.Host to be 'localhost', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 3306 {
			t.Errorf("Expected Database.Port to be 3306, got %d", config.Database.Port)
		}
		if config.Server.Port != 8080 {
			t.Errorf("Expected Server.Port to be 8080, got %d", config.Server.Port)
		}
		if !config.Features["logging"] {
			t.Errorf("Expected Features['logging'] to be true")
		}
		if config.Features["metrics"] {
			t.Errorf("Expected Features['metrics'] to be false")
		}
	})

	// 使用 WithFile 加载配置
	t.Run("WithFile", func(t *testing.T) {
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(configPath, "yaml"),
		)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "localhost" {
			t.Errorf("Expected Database.Host to be 'localhost', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 3306 {
			t.Errorf("Expected Database.Port to be 3306, got %d", config.Database.Port)
		}
	})

	// 使用 WithFile 自动推断类型
	t.Run("WithFileAutoDetect", func(t *testing.T) {
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(configPath, ""),
		)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "localhost" {
			t.Errorf("Expected Database.Host to be 'localhost', got '%s'", config.Database.Host)
		}
	})
}

// 命令行参数测试
func TestFlagSource(t *testing.T) {
	// 创建临时配置文件
	configPath := filepath.Join(t.TempDir(), "config.yml")
	configContent := `
database:
  host: flag-test-host
  port: 5432
  username: admin
  password: secret
server:
  port: 9090
  timeout: 60
features:
  logging: true
  metrics: true
`
	testutil.CreateTestConfigFile(t, configPath, configContent)

	// 使用环境变量模拟命令行参数
	// 因为在测试环境中直接修改 os.Args 可能会影响其他测试
	t.Run("FlagSourceViaEnv", func(t *testing.T) {
		// 设置环境变量来模拟命令行参数
		flagName := "test-config"
		os.Setenv("FLAG_"+strings.ToUpper(flagName), configPath)
		defer os.Unsetenv("FLAG_" + strings.ToUpper(flagName))

		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFlag(flagName, "", "yaml"),
		)
		if err != nil {
			t.Fatalf("Failed to load config from flag: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "flag-test-host" {
			t.Errorf("Expected Database.Host to be 'flag-test-host', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 5432 {
			t.Errorf("Expected Database.Port to be 5432, got %d", config.Database.Port)
		}
		if config.Server.Port != 9090 {
			t.Errorf("Expected Server.Port to be 9090, got %d", config.Server.Port)
		}
		if !config.Features["metrics"] {
			t.Errorf("Expected Features['metrics'] to be true")
		}
	})

	// 测试默认路径
	t.Run("FlagSourceWithDefault", func(t *testing.T) {
		// 清除可能存在的环境变量
		os.Unsetenv("FLAG_TEST_CONFIG")

		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFlag("test-config", configPath, "yaml"),
		)
		if err != nil {
			t.Fatalf("Failed to load config from flag with default: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "flag-test-host" {
			t.Errorf("Expected Database.Host to be 'flag-test-host', got '%s'", config.Database.Host)
		}
	})
}

// 环境变量测试
func TestEnvSource(t *testing.T) {
	// 创建临时配置文件
	configPath := filepath.Join(t.TempDir(), "env-config.yml")
	configContent := `
database:
  host: env-test-host
  port: 7777
  username: env-user
  password: env-password
server:
  port: 8888
  timeout: 45
features:
  logging: false
  metrics: true
  tracing: true
`
	// 创建测试配置文件
	testutil.CreateTestConfigFile(t, configPath, configContent)

	// 测试从环境变量加载配置
	t.Run("EnvVarSource", func(t *testing.T) {
		// 设置环境变量
		envVar := "TEST_CONFIG_PATH"
		os.Setenv(envVar, configPath)
		defer os.Unsetenv(envVar)

		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithEnv(envVar, "yaml"),
		)
		if err != nil {
			t.Fatalf("Failed to load config from env var: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "env-test-host" {
			t.Errorf("Expected Database.Host to be 'env-test-host', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 7777 {
			t.Errorf("Expected Database.Port to be 7777, got %d", config.Database.Port)
		}
		if config.Server.Port != 8888 {
			t.Errorf("Expected Server.Port to be 8888, got %d", config.Server.Port)
		}
		if config.Features["logging"] {
			t.Errorf("Expected Features['logging'] to be false")
		}
		if !config.Features["tracing"] {
			t.Errorf("Expected Features['tracing'] to be true")
		}
	})

	// 测试环境变量未设置的情况
	t.Run("EnvVarNotSet", func(t *testing.T) {
		envVar := "NONEXISTENT_CONFIG_PATH"
		os.Unsetenv(envVar) // 确保环境变量未设置

		// 提供一个默认配置文件作为备选
		defaultConfigPath := filepath.Join(t.TempDir(), "default-config.yml")
		defaultConfigContent := `
database:
  host: default-host
  port: 1234
`
		testutil.CreateTestConfigFile(t, defaultConfigPath, defaultConfigContent)

		// 尝试从环境变量加载，但提供备选文件
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithEnv(envVar, "yaml"),
			cyconf.WithFile(defaultConfigPath, "yaml"),
		)
		if err != nil {
			t.Fatalf("Failed to load config with fallback: %v", err)
		}

		// 验证配置值应该来自默认文件
		if config.Database.Host != "default-host" {
			t.Errorf("Expected Database.Host to be 'default-host', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 1234 {
			t.Errorf("Expected Database.Port to be 1234, got %d", config.Database.Port)
		}
	})
}

// 多文件合并测试
func TestFilesMerge(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()

	// 创建基础配置文件
	baseConfigPath := filepath.Join(tempDir, "base.yml")
	baseConfigContent := `
database:
  host: base-host
  port: 3306
  username: base-user
  password: base-password
server:
  port: 8080
  timeout: 30
features:
  logging: true
`
	testutil.CreateTestConfigFile(t, baseConfigPath, baseConfigContent)

	// 创建覆盖配置文件
	overrideConfigPath := filepath.Join(tempDir, "override.yml")
	overrideConfigContent := `
database:
  host: override-host
  port: 5432
server:
  timeout: 60
features:
  metrics: true
`
	testutil.CreateTestConfigFile(t, overrideConfigPath, overrideConfigContent)

	// 测试 WithFiles 函数
	t.Run("WithFiles", func(t *testing.T) {
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFiles("yaml", baseConfigPath, overrideConfigPath),
		)
		if err != nil {
			t.Fatalf("Failed to load config with WithFiles: %v", err)
		}

		// 验证配置值
		// 数据库主机应该被覆盖
		if config.Database.Host != "override-host" {
			t.Errorf("Expected Database.Host to be 'override-host', got '%s'", config.Database.Host)
		}
		// 数据库端口应该被覆盖
		if config.Database.Port != 5432 {
			t.Errorf("Expected Database.Port to be 5432, got %d", config.Database.Port)
		}
		// 用户名不应该被覆盖
		if config.Database.Username != "base-user" {
			t.Errorf("Expected Database.Username to be 'base-user', got '%s'", config.Database.Username)
		}
		// 服务器端口不应该被覆盖
		if config.Server.Port != 8080 {
			t.Errorf("Expected Server.Port to be 8080, got %d", config.Server.Port)
		}
		// 超时应该被覆盖
		if config.Server.Timeout != 60 {
			t.Errorf("Expected Server.Timeout to be 60, got %d", config.Server.Timeout)
		}
		// 日志功能不应该被覆盖
		if !config.Features["logging"] {
			t.Errorf("Expected Features['logging'] to be true")
		}
		// 指标功能应该被添加
		if !config.Features["metrics"] {
			t.Errorf("Expected Features['metrics'] to be true")
		}
	})

	// 测试多种文件类型
	t.Run("MultipleFileTypes", func(t *testing.T) {
		// 创建 JSON 配置文件
		jsonConfigPath := filepath.Join(tempDir, "config.json")
		jsonConfigContent := `{
  "database": {
    "host": "json-host",
    "port": 9999
  },
  "features": {
    "json": true
  }
}`
		testutil.CreateTestConfigFile(t, jsonConfigPath, jsonConfigContent)

		// 使用自动类型检测
		config, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFiles("", baseConfigPath, jsonConfigPath),
		)
		if err != nil {
			t.Fatalf("Failed to load config with multiple file types: %v", err)
		}

		// 验证配置值
		if config.Database.Host != "json-host" {
			t.Errorf("Expected Database.Host to be 'json-host', got '%s'", config.Database.Host)
		}
		if config.Database.Port != 9999 {
			t.Errorf("Expected Database.Port to be 9999, got %d", config.Database.Port)
		}
		if !config.Features["json"] {
			t.Errorf("Expected Features['json'] to be true")
		}
	})
}

// 错误处理测试
func TestErrorHandling(t *testing.T) {
	// 测试不存在的配置文件
	t.Run("NonExistentFile", func(t *testing.T) {
		nonExistentPath := "/path/to/nonexistent/config.yml"
		_, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(nonExistentPath, "yaml"),
		)
		if err == nil {
			t.Error("Expected error for non-existent file, but got nil")
		}
	})

	// 测试无效的配置类型
	t.Run("InvalidConfigType", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yml")
		configContent := "database:\n  host: localhost"
		testutil.CreateTestConfigFile(t, configPath, configContent)

		_, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(configPath, "invalid-type"),
		)
		if err == nil {
			t.Error("Expected error for invalid config type, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "invalid config type") {
			t.Errorf("Expected error message to contain 'invalid config type', but got: %v", err)
		}
	})

	// 测试格式错误的配置文件
	t.Run("MalformedConfig", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "malformed.yml")
		malformedContent := `
database:
  host: "localhost
  port: 3306
` // 注意这里缺少闭合引号
		testutil.CreateTestConfigFile(t, configPath, malformedContent)

		_, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(configPath, "yaml"),
		)
		if err == nil {
			t.Error("Expected error for malformed config, but got nil")
		}
	})

	// 测试类型不匹配的配置
	t.Run("TypeMismatch", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "type-mismatch.yml")
		mismatchContent := `
database:
  host: localhost
  port: "not-a-number" # 应该是整数，但这里是字符串
`
		testutil.CreateTestConfigFile(t, configPath, mismatchContent)

		_, err := cyconf.LoadConfig[testutil.TestConfig](
			cyconf.WithFile(configPath, "yaml"),
		)
		if err == nil {
			t.Error("Expected error for type mismatch, but got nil")
		}
	})

	// 测试无配置源
	t.Run("NoConfigSource", func(t *testing.T) {
		// 清除所有可能的环境变量
		os.Unsetenv("CONFIG_PATH")
		os.Unsetenv("FLAG_CONFIG")

		// 不提供任何配置源
		_, err := cyconf.LoadConfig[testutil.TestConfig]()
		if err == nil {
			t.Error("Expected error when no config source is available, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "no valid config found") {
			t.Errorf("Expected error message to contain 'no valid config found', but got: %v", err)
		}
	})
}

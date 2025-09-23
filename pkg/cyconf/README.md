# cyconf 配置系统使用文档

`cyconf` 是一个灵活的配置管理系统，支持从多种来源加载配置，包括单个文件、目录和多个搜索路径。本文档详细介绍了所有配置选项及其用法。

## 基本用法

最简单的用法是使用默认选项加载配置：

```go
type Config struct {
    Database struct {
        Host     string
        Port     int
        Username string
        Password string
    }
    Server struct {
        Port    int
        Timeout int
    }
}

// 使用默认选项加载配置（从 ./config.yml）
config, err := cyconf.LoadConfig[Config]()
if err != nil {
    log.Fatalf("Failed to load config: %v", err)
}

fmt.Printf("Database host: %s\n", config.Database.Host)
```

## 配置选项

`cyconf` 提供了多种配置选项，可以通过函数选项模式进行设置：

### WithPath

设置直接配置路径（最高优先级）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config.yaml")
)
```

### WithFlag

指定命令行标志名称（例如 `--config`）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFlag("my-config")
)
```

### WithEnv

指定环境变量名称（例如 `APP_CONFIG`）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithEnv("APP_CONFIG")
)
```

### WithDefault

设置默认配置文件路径（最低优先级）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithDefault("./configs/default.yaml")
)
```

### WithConfigType

显式设置配置类型（例如 "yaml"、"json"）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithConfigType("json")
)
```

### WithFile

添加一个配置文件（支持多个，自动合并）。如果指定了类型，会进行验证；如果未指定类型，将自动从文件扩展名推断。

```go
// 指定类型
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFile("./config.json", "json")
)

// 自动推断类型
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFile("./config.json", "")
)

// 如果指定了无效的类型，将返回错误
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFile("./config.json", "invalid-type")
)
// 错误: "config validation error: invalid config for path ./config.json: invalid config type: invalid-type"
```

### WithFiles

添加多个配置文件（合并）。同样支持类型验证和自动推断。

```go
// 所有文件使用相同的类型
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFiles("json", "./config1.json", "./config2.json")
)

// 自动推断每个文件的类型
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFiles("", "./config.json", "./settings.yaml")
)
```

### WithSupportDir

启用目录支持，允许从目录中加载配置文件。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config/dir"),
    cyconf.WithSupportDir(true)
)
```

### WithConfigName

设置配置文件的基本名称（不包括扩展名）。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config/dir"),
    cyconf.WithSupportDir(true),
    cyconf.WithConfigName("app")
)
```

### WithSearchPaths

指定多个搜索路径来查找配置文件。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithSearchPaths([]string{
        "./config",
        "/etc/app",
        "/usr/local/etc/app",
    }),
    cyconf.WithConfigName("settings")
)
```

### WithMergeStrategy

设置多文件配置的合并策略。

```go
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config/dir"),
    cyconf.WithSupportDir(true),
    cyconf.WithMergeStrategy(cyconf.MergeStrategyDeep)
)
```

## 配置加载优先级

配置加载遵循以下优先级（从高到低）：

1. 直接路径（WithPath）
2. 命令行标志（WithFlag，如果未指定则默认为 "--config" 标志）
3. 环境变量（WithEnv）
4. 默认路径（WithDefault，如果未指定则默认为 "./config.yml"）

## 多文件配置

`cyconf` 支持配置文件合并，允许从多个文件加载配置并将它们合并成一个完整的配置。

当使用 `WithFiles` 选项指定多个配置文件时，后面的文件会覆盖前面文件中的相同配置项。这种合并是深度的，意味着嵌套的配置项也会被正确合并。例如：

**base.yml**:
```yaml
database:
  host: base-host
  port: 3306
  username: base-user
server:
  port: 8080
```

**override.yml**:
```yaml
database:
  host: override-host
  port: 5432
```

合并后的配置将是：
```yaml
database:
  host: override-host  # 从 override.yml
  port: 5432           # 从 override.yml
  username: base-user  # 从 base.yml
server:
  port: 8080           # 从 base.yml
```

这种合并行为允许您使用基础配置文件定义通用设置，然后使用特定的覆盖文件来修改某些值，而不需要重复所有配置。

## 环境变量支持

`cyconf` 自动支持环境变量覆盖配置值。环境变量名称应使用下划线替换点，例如 `DATABASE_HOST` 可以覆盖 `database.host` 配置项。

## 完整示例

```go
package main

import (
    "fmt"
    "log"

    "github.com/yourusername/yourproject/pkg/cyconf"
)

type Config struct {
    Database struct {
        Host     string
        Port     int
        Username string
        Password string
    }
    Server struct {
        Port    int
        Timeout int
    }
    Features map[string]bool
}

func main() {
    // 从多个搜索路径加载配置，使用深度合并策略
    config, err := cyconf.LoadConfig[Config](
        cyconf.WithSearchPaths([]string{"./config", "/etc/app"}),
        cyconf.WithConfigName("app"),
        cyconf.WithMergeStrategy(cyconf.MergeStrategyDeep),
    )
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    fmt.Printf("Database: %s:%d\n", config.Database.Host, config.Database.Port)
    fmt.Printf("Server port: %d\n", config.Server.Port)
    fmt.Printf("Features: %v\n", config.Features)
}
```

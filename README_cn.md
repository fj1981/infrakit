# pk-infrakit-g

基础设施工具包，提供了一系列用于构建Go应用程序的工具包。

## 包概述

### cyconf - 配置系统

`cyconf` 是一个灵活的配置管理系统，支持从多种来源加载配置，包括单个文件、目录和多个搜索路径。

**主要功能**:
- 支持多种配置源（文件、环境变量、命令行参数）
- 支持多种配置格式（YAML、JSON等）
- 支持配置合并和覆盖
- 支持环境变量覆盖配置值

**使用示例**:
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

// 使用自定义路径
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config.yaml")
)

// 使用多个配置文件（合并）
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFiles("", "./config.json", "./settings.yaml")
)
```

### cydb - 数据库操作

`cydb` 提供了数据库操作的抽象层，支持多种数据库类型（MySQL、PostgreSQL、Oracle、SQLite）。

**主要功能**:
- 统一的数据库连接配置
- 支持多种数据库类型
- SQL方言转换
- 数据格式化和转换

**使用示例**:
```go
// 配置数据库连接
dbConfig := cydb.DBConnection{
    Key:    "primary_db",
    Type:   "mysql",
    Host:   "localhost",
    Port:   3306,
    Un:     "user",
    Pw:     "password",
    DBName: "mydb",
}

// 使用数据库客户端
client := cydb.NewDatabaseClient(dbConfig)
rows, err := client.Query("SELECT * FROM users WHERE id = ?", 1)
```

### cydist - 分布式系统工具

`cydist` 包提供了分布式系统相关的工具，包括基于Redis的内存记录系统。

**主要功能**:
- 基于Redis的内存记录系统
- 支持设置记录超时时间和最大记录数
- 支持命名空间隔离
- 本地缓存机制

**使用示例**:
```go
// 创建Redis客户端
client := cydist.New(cydist.WithAddr("localhost:6379"))

// 创建MemoryStore实例
ms := cydist.NewMemoryStore(client, "myservice")

// 配置key
key := "user_actions"
expire := 10 * time.Minute
maxRecords := int64(100)
err := ms.CreateKey(context.Background(), key, expire, maxRecords)

// 添加记录
data := map[string]interface{}{
    "user_id": 12345,
    "action": "login",
    "timestamp": time.Now(),
}
err = ms.WriteRecord(context.Background(), key, data)

// 获取记录
records, err := ms.GetLatestRecords(context.Background(), key, 10)
```

### cyfct - 工厂模式工具

`cyfct` 包提供了单例模式和工厂模式的实现，用于管理应用程序中的对象实例。

**主要功能**:
- 单例模式实现
- 支持按类型注册和获取实例
- 支持分组管理实例

**使用示例**:
```go
// 注册工厂函数
cyfct.RegisterFactory(func() (MyService, error) {
    return &myServiceImpl{}, nil
})

// 获取实例
service, err := cyfct.GetInstance[MyService]()

// 注册带分组的工厂函数
cyfct.RegisterFactoryWithGroup("services", func() (MyService, error) {
    return &myServiceImpl{}, nil
})

// 获取分组中的所有实例
services, err := cyfct.GetInstancesByGroup[MyService]("services")
```

### cygin - Web框架

`cygin` 包是基于Gin框架的Web服务器封装，提供了更便捷的API定义和路由管理。

**主要功能**:
- 基于Gin的Web服务器
- 支持API分组和路由管理
- 支持Swagger文档生成
- 支持静态文件服务
- 支持CORS、健康检查等中间件

**使用示例**:
```go
// 创建服务器
server := cygin.NewServer(
    cygin.WithPort(8080),
    cygin.WithCORS(),
    cygin.WithHealthCheck(),
    cygin.WithVersionInfo(),
    cygin.WithSwagger(),
)

// 添加API组
server.AddApiGroup(cygin.ApiGroup{
    BasePath: "/api/v1",
    Endpoints: []cygin.ApiEndpoint{
        {
            Path:    "/users",
            Method:  cygin.Get,
            Summary: "获取用户列表",
            Handler: GetUsers,
        },
    },
})

// 启动服务器
server.Run(context.Background())
```

### cylog - 日志系统

`cylog` 包提供了基于Go标准库slog的日志系统，支持多种输出格式和日志级别。

**主要功能**:
- 基于slog的结构化日志
- 支持多种输出格式（文本、JSON、Apache格式）
- 支持日志轮转
- 支持调用者信息记录

**使用示例**:
```go
// 初始化默认日志
cylog.InitDefault(
    cylog.WithLevel(slog.LevelInfo),
    cylog.WithFormat("json"),
    cylog.WithFilename("app.log"),
    cylog.WithMaxSize(10), // MB
)

// 使用日志
cylog.Info("服务启动", "port", 8080)
cylog.Debug("调试信息")
cylog.Warn("警告信息")
err := cylog.Error("错误信息", "code", 500)

// 创建自定义日志器
logger := cylog.New(
    cylog.WithLevel(slog.LevelDebug),
    cylog.WithAddSource(true),
)
logger.Info("自定义日志器")
```

### cyswag - Swagger文档生成

`cyswag` 包提供了Swagger文档生成工具，用于自动生成API文档。

**主要功能**:
- 自动生成Swagger文档
- 支持API分组和标签
- 支持请求和响应模型定义

**使用示例**:
```go
// 注册API端点
cyswag.Register(
    cyswag.WithTitle("My API"),
    cyswag.WithVersion("1.0.0"),
    cyswag.WithDescription("API文档"),
    cyswag.WithApiEndpoint(
        cyswag.APIEndpoint{
            Path:        "/api/users",
            Method:      "GET",
            Summary:     "获取用户列表",
            Description: "获取系统中的所有用户",
            Tags:        []string{"用户管理"},
        },
    ),
)
```

### cyutil - 通用工具

`cyutil` 包提供了各种通用工具函数。

**主要功能**:
- 字符串处理
- 时间格式化
- 类型转换
- 其他实用工具

**使用示例**:
```go
// 字符串转换
str := cyutil.ToStr(123)

// 时间格式化
duration := cyutil.FormatDuration(5 * time.Second)

// 用户代理解析
uaInfo := cyutil.ParseUserAgent(userAgentString)
```

## 使用示例

以下是一个使用多个包构建Web应用的示例：

```go
package main

import (
    "context"
    "log"

    "github.com/fj1981/infrakit/pkg/cyconf"
    "github.com/fj1981/infrakit/pkg/cydb"
    "github.com/fj1981/infrakit/pkg/cygin"
    "github.com/fj1981/infrakit/pkg/cylog"
)

type Config struct {
    Server struct {
        Port int
    }
    Database cydb.DBConnection
}

func main() {
    // 初始化日志
    cylog.InitDefault(
        cylog.WithLevel(slog.LevelInfo),
        cylog.WithFilename("app.log"),
    )

    // 加载配置
    config, err := cyconf.LoadConfig[Config](
        cyconf.WithSearchPaths([]string{"./config", "/etc/app"}),
        cyconf.WithConfigName("app"),
    )
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    // 创建数据库客户端
    dbClient := cydb.NewDatabaseClient(config.Database)

    // 创建Web服务器
    server := cygin.NewServer(
        cygin.WithPort(config.Server.Port),
        cygin.WithCORS(),
        cygin.WithHealthCheck(),
        cygin.WithVersionInfo(),
        cygin.WithSwagger(),
    )

    // 添加API路由
    server.AddApiGroup(cygin.ApiGroup{
        BasePath: "/api/v1",
        Endpoints: []cygin.ApiEndpoint{
            {
                Path:    "/users",
                Method:  cygin.Get,
                Summary: "获取用户列表",
                Handler: func() ([]User, error) {
                    // 使用dbClient查询数据库
                    return getUsers(dbClient)
                },
            },
        },
    })

    // 启动服务器
    if err := server.Run(context.Background()); err != nil {
        cylog.Error("Server failed", "error", err)
    }
}
```

## 贡献

欢迎提交问题和拉取请求。对于重大更改，请先打开一个问题讨论您想要更改的内容。

## 许可证

[许可证信息]

# pk-infrakit-g

Infrastructure toolkit providing a series of packages for building Go applications.

## Package Overview

### cyconf - Configuration System

`cyconf` is a flexible configuration management system that supports loading configurations from various sources, including single files, directories, and multiple search paths.

**Key Features**:
- Support for multiple configuration sources (files, environment variables, command-line arguments)
- Support for multiple configuration formats (YAML, JSON, etc.)
- Configuration merging and overriding
- Environment variable override for configuration values

**Usage Example**:
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

// Load configuration using default options (from ./config.yml)
config, err := cyconf.LoadConfig[Config]()
if err != nil {
    log.Fatalf("Failed to load config: %v", err)
}

// Using a custom path
config, err := cyconf.LoadConfig[Config](
    cyconf.WithPath("/path/to/config.yaml")
)

// Using multiple configuration files (merged)
config, err := cyconf.LoadConfig[Config](
    cyconf.WithFiles("", "./config.json", "./settings.yaml")
)
```

### cydb - Database Operations

`cydb` provides an abstraction layer for database operations, supporting multiple database types (MySQL, PostgreSQL, Oracle, SQLite).

**Key Features**:
- Unified database connection configuration
- Support for multiple database types
- SQL dialect conversion
- Data formatting and conversion

**Usage Example**:
```go
// Configure database connection
dbConfig := cydb.DBConnection{
    Key:    "primary_db",
    Type:   "mysql",
    Host:   "localhost",
    Port:   3306,
    Un:     "user",
    Pw:     "password",
    DBName: "mydb",
}

// Use database client
client := cydb.NewDatabaseClient(dbConfig)
rows, err := client.Query("SELECT * FROM users WHERE id = ?", 1)
```

### cydist - Distributed System Tools

`cydist` package provides tools related to distributed systems, including a Redis-based memory record system.

**Key Features**:
- Redis-based memory record system
- Support for setting record timeout and maximum record count
- Namespace isolation
- Local caching mechanism

**Usage Example**:
```go
// Create Redis client
client := cydist.New(cydist.WithAddr("localhost:6379"))

// Create MemoryStore instance
ms := cydist.NewMemoryStore(client, "myservice")

// Configure key
key := "user_actions"
expire := 10 * time.Minute
maxRecords := int64(100)
err := ms.CreateKey(context.Background(), key, expire, maxRecords)

// Add record
data := map[string]interface{}{
    "user_id": 12345,
    "action": "login",
    "timestamp": time.Now(),
}
err = ms.WriteRecord(context.Background(), key, data)

// Get records
records, err := ms.GetLatestRecords(context.Background(), key, 10)
```

### cyfct - Factory Pattern Tools

`cyfct` package provides implementations of singleton and factory patterns for managing object instances in applications.

**Key Features**:
- Singleton pattern implementation
- Support for registering and retrieving instances by type
- Support for group management of instances

**Usage Example**:
```go
// Register factory function
cyfct.RegisterFactory(func() (MyService, error) {
    return &myServiceImpl{}, nil
})

// Get instance
service, err := cyfct.GetInstance[MyService]()

// Register factory function with group
cyfct.RegisterFactoryWithGroup("services", func() (MyService, error) {
    return &myServiceImpl{}, nil
})

// Get all instances in a group
services, err := cyfct.GetInstancesByGroup[MyService]("services")
```

### cygin - Web Framework

`cygin` package is a wrapper around the Gin framework, providing more convenient API definition and route management.

**Key Features**:
- Gin-based web server
- Support for API grouping and route management
- Swagger documentation generation
- Static file serving
- Middleware support (CORS, health check, etc.)

**Usage Example**:
```go
// Create server
server := cygin.NewServer(
    cygin.WithPort(8080),
    cygin.WithCORS(),
    cygin.WithHealthCheck(),
    cygin.WithVersionInfo(),
    cygin.WithSwagger(),
)

// Add API group
server.AddApiGroup(cygin.ApiGroup{
    BasePath: "/api/v1",
    Endpoints: []cygin.ApiEndpoint{
        {
            Path:    "/users",
            Method:  cygin.Get,
            Summary: "Get user list",
            Handler: GetUsers,
        },
    },
})

// Start server
server.Run(context.Background())
```

### cylog - Logging System

`cylog` package provides a logging system based on Go's standard slog library, supporting multiple output formats and log levels.

**Key Features**:
- Structured logging based on slog
- Support for multiple output formats (text, JSON, Apache format)
- Log rotation
- Caller information recording

**Usage Example**:
```go
// Initialize default logger
cylog.InitDefault(
    cylog.WithLevel(slog.LevelInfo),
    cylog.WithFormat("json"),
    cylog.WithFilename("app.log"),
    cylog.WithMaxSize(10), // MB
)

// Use logger
cylog.Info("Server started", "port", 8080)
cylog.Debug("Debug information")
cylog.Warn("Warning message")
err := cylog.Error("Error message", "code", 500)

// Create custom logger
logger := cylog.New(
    cylog.WithLevel(slog.LevelDebug),
    cylog.WithAddSource(true),
)
logger.Info("Custom logger")
```

### cyswag - Swagger Documentation Generation

`cyswag` package provides tools for generating Swagger documentation automatically.

**Key Features**:
- Automatic Swagger documentation generation
- Support for API grouping and tagging
- Support for request and response model definition

**Usage Example**:
```go
// Register API endpoint
cyswag.Register(
    cyswag.WithTitle("My API"),
    cyswag.WithVersion("1.0.0"),
    cyswag.WithDescription("API Documentation"),
    cyswag.WithApiEndpoint(
        cyswag.APIEndpoint{
            Path:        "/api/users",
            Method:      "GET",
            Summary:     "Get user list",
            Description: "Get all users in the system",
            Tags:        []string{"User Management"},
        },
    ),
)
```

### cyutil - General Utilities

`cyutil` package provides various general utility functions.

**Key Features**:
- String processing
- Time formatting
- Type conversion
- Other utility tools

**Usage Example**:
```go
// String conversion
str := cyutil.ToStr(123)

// Time formatting
duration := cyutil.FormatDuration(5 * time.Second)

// User agent parsing
uaInfo := cyutil.ParseUserAgent(userAgentString)
```

## Usage Example

Here's an example of building a web application using multiple packages:

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
    // Initialize logging
    cylog.InitDefault(
        cylog.WithLevel(slog.LevelInfo),
        cylog.WithFilename("app.log"),
    )

    // Load configuration
    config, err := cyconf.LoadConfig[Config](
        cyconf.WithSearchPaths([]string{"./config", "/etc/app"}),
        cyconf.WithConfigName("app"),
    )
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    // Create database client
    dbClient := cydb.NewDatabaseClient(config.Database)

    // Create web server
    server := cygin.NewServer(
        cygin.WithPort(config.Server.Port),
        cygin.WithCORS(),
        cygin.WithHealthCheck(),
        cygin.WithVersionInfo(),
        cygin.WithSwagger(),
    )

    // Add API routes
    server.AddApiGroup(cygin.ApiGroup{
        BasePath: "/api/v1",
        Endpoints: []cygin.ApiEndpoint{
            {
                Path:    "/users",
                Method:  cygin.Get,
                Summary: "Get user list",
                Handler: func() ([]User, error) {
                    // Use dbClient to query database
                    return getUsers(dbClient)
                },
            },
        },
    })

    // Start server
    if err := server.Run(context.Background()); err != nil {
        cylog.Error("Server failed", "error", err)
    }
}
```

## Contributing

Issues and pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[License Information]

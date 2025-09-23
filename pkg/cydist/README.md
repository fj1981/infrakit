# MemoryStore - 基于 Redis 的内存记录系统

MemoryStore 是一个基于 Redis 的内存记录系统，允许您根据某个 key 记录多条记录，并提供以下功能：

1. 为每个 key 设置整体超时时间
2. 重新写入时重新计算超期时间
3. 获取最新的多条数据
4. 为每个 key 设置记录上限，超过的记录会被丢弃
5. 支持命名空间隔离，确保多个服务间的数据不会相互干扰
6. 配置信息持久化存储在 Redis 中，服务重启后配置不会丢失
7. 本地缓存机制，提高配置获取性能

## 功能特性

- **配置管理**：可以预先为 key 设置配置（超时时间和最大记录数），配置信息持久化存储在 Redis 中
- **记录存储**：支持向指定 key 添加记录
- **记录获取**：支持获取指定 key 的最新记录
- **记录限制**：自动维护每个 key 的最大记录数
- **自动过期**：支持为每个 key 设置过期时间
- **命名空间隔离**：支持通过命名空间隔离不同服务的数据
- **持久化配置**：配置信息存储在 Redis 中，服务重启后不会丢失
- **本地缓存**：配置信息在本地缓存，提高获取性能，默认缓存5分钟

## 安装和使用

### 1. 创建 MemoryStore 实例

```go
import "github.com/fj1981/infrakit/pkg/cydist"

// 创建 Redis 客户端
client := cydist.New(cydist.WithAddr("localhost:6379"))

// 创建 MemoryStore 实例，指定命名空间
ms := cydist.NewMemoryStore(client, "myservice")
```

### 2. 配置缓存设置（可选）

```go
// 设置配置缓存的过期时间
ms.SetConfigCacheTTL(10 * time.Minute)
```

### 3. 预先配置 key（推荐方式）

```go
// 为 key 配置超时时间和最大记录数
key := "user_actions"
expire := 10 * time.Minute
maxRecords := int64(100)

err := ms.CreateKey(context.Background(), key, expire, maxRecords)
if err != nil {
    // 处理错误
}

// 添加记录（无需每次都指定配置）
data := map[string]interface{}{
    "user_id": 12345,
    "action": "login",
    "timestamp": time.Now(),
}

err = ms.WriteRecord(context.Background(), key, data)
if err != nil {
    // 处理错误
}
```

### 4. 直接写入记录（向后兼容方式）

```go
// 也可以直接指定配置参数（向后兼容）
key := "user_actions"
data := map[string]interface{}{
    "user_id": 12345,
    "action": "login",
    "timestamp": time.Now(),
}
expire := 10 * time.Minute
maxRecords := int64(100)

err := ms.WriteRecordWithConfig(context.Background(), key, data, expire, maxRecords)
if err != nil {
    // 处理错误
}
```

### 5. 获取记录

```go
// 获取最新的 10 条记录
records, err := ms.GetLatestRecords(context.Background(), key, 10)
if err != nil {
    // 处理错误
}

for _, record := range records {
    fmt.Printf("Data: %v, Timestamp: %v\n", record.Data, record.Timestamp)
}
```

### 6. 删除记录

```go
// 删除指定 key 的所有记录和配置
deleted, err := ms.DeleteRecords(context.Background(), key)
if err != nil {
    // 处理错误
}
```

## API 参考

### type MemoryStore

```go
type MemoryStore struct {
    // contains filtered or unexported fields
}
```

#### func NewMemoryStore

```go
func NewMemoryStore(client *RedisClient, namespace string) *MemoryStore
```

NewMemoryStore 创建一个新的 MemoryStore 实例，namespace 参数用于隔离不同服务的数据。

#### func (*MemoryStore) SetConfigCacheTTL

```go
func (ms *MemoryStore) SetConfigCacheTTL(ttl time.Duration)
```

SetConfigCacheTTL 设置配置缓存的过期时间，默认为5分钟。

#### func (*MemoryStore) CreateKey

```go
func (ms *MemoryStore) CreateKey(ctx context.Context, key string, expire time.Duration, maxRecords int64) error
```

CreateKey 为指定的 key 创建配置，包括过期时间和最大记录数。配置信息会持久化存储在 Redis 中，并在本地缓存以提高性能。

#### func (*MemoryStore) DeleteKey

```go
func (ms *MemoryStore) DeleteKey(ctx context.Context, key string) (int64, error)
```

DeleteKey 删除指定 key 的配置，并从本地缓存中移除。

#### func (*MemoryStore) WriteRecord

```go
func (ms *MemoryStore) WriteRecord(ctx context.Context, key string, data interface{}) error
```

WriteRecord 向预先配置的 key 添加一条记录。配置信息会优先从本地缓存获取，如果缓存未命中或已过期，则从 Redis 中获取。

#### func (*MemoryStore) WriteRecordWithConfig

```go
func (ms *MemoryStore) WriteRecordWithConfig(ctx context.Context, key string, data interface{}, expire time.Duration, maxRecords int64) error
```

WriteRecordWithConfig 向指定 key 添加一条记录，并指定配置（向后兼容方法）。

#### func (*MemoryStore) GetLatestRecords

```go
func (ms *MemoryStore) GetLatestRecords(ctx context.Context, key string, count int64) ([]Record, error)
```

GetLatestRecords 获取指定 key 的最新记录。

#### func (*MemoryStore) GetRecordCount

```go
func (ms *MemoryStore) GetRecordCount(ctx context.Context, key string) (int64, error)
```

GetRecordCount 获取指定 key 的记录数量。

#### func (*MemoryStore) DeleteRecords

```go
func (ms *MemoryStore) DeleteRecords(ctx context.Context, key string) (int64, error)
```

DeleteRecords 删除指定 key 的所有记录和配置。

### type Record

```go
type Record struct {
    Data      interface{} `json:"data"`
    Timestamp time.Time   `json:"timestamp"`
}
```

Record 表示一条记录。

### type KeyNotConfiguredError

```go
type KeyNotConfiguredError struct {
    Key string
}
```

KeyNotConfiguredError 表示 key 未配置的错误。

## 缓存机制说明

MemoryStore 使用本地缓存来提高配置获取的性能：

1. 当调用 [WriteRecord](file:///Users/fanjun/project/pk-infrakit-g/pkg/cydist/memorystore.go#L248-L283) 时，首先尝试从本地缓存获取配置信息
2. 如果缓存未命中或已过期，则从 Redis 中获取配置信息，并更新本地缓存
3. 默认缓存时间为5分钟，可以通过 [SetConfigCacheTTL](file:///Users/fanjun/project/pk-infrakit-g/pkg/cydist/memorystore.go#L39-L44) 方法调整
4. MemoryStore 会定期清理过期的缓存项

## 注意事项

1. 使用前请确保 Redis 服务正在运行
2. 推荐使用 [CreateKey](file:///Users/fanjun/project/pk-infrakit-g/pkg/cydist/memorystore.go#L46-L72) 和 [WriteRecord](file:///Users/fanjun/project/pk-infrakit-g/pkg/cydist/memorystore.go#L248-L283) 方法，这样不需要每次都指定配置参数
3. 当使用 [WriteRecordWithConfig](file:///Users/fanjun/project/pk-infrakit-g/pkg/cydist/memorystore.go#L285-L295) 方法时，配置仅在该次调用中有效
4. 记录会自动按时间倒序存储（最新的记录在最前面）
5. 当记录数超过限制时，旧的记录会被自动删除
6. 使用命名空间可以有效隔离不同服务的数据，避免相互干扰
7. 配置信息持久化存储在 Redis 中，服务重启后配置不会丢失
8. 本地缓存机制可以显著提高配置获取性能，减少 Redis 访问次数
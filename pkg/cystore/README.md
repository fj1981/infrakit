# CyStore - 统一云存储接口

CyStore 提供了一个统一的云存储接口，支持多种云存储服务，如 MinIO、S3、华为云 OBS、阿里云 OSS 等。

## 特性

- 统一的接口定义，支持多种云存储服务
- 简单易用的 API，支持常见的存储操作
- 支持预签名 URL 生成
- 支持元数据管理
- 支持桶（Bucket）管理
- 支持华为云 OBS 和阿里云 OSS

## 安装

首先，确保已添加所需的客户端依赖：

```bash
# MinIO 客户端
go get github.com/minio/minio-go/v7

# 华为云 OBS 客户端
go get github.com/huaweicloud/huaweicloud-sdk-go-obs/obs

# 阿里云 OSS 客户端
go get github.com/aliyun/aliyun-oss-go-sdk/oss
```

## 配置示例

新的配置结构已经统一，只需要指定一个提供商类型和对应的公共配置即可。

### MinIO 配置

```go
import (
    "github.com/fj1981/infrakit/pkg/cystore"
)

config := &cystore.Config{
    Provider:   cystore.ProviderMinio,
    Region:     "us-east-1",
    Secure:     true,
    Endpoint:   "play.min.io",
    AccessKey:  "your-access-key",
    SecretKey:  "your-secret-key",
    UseSSL:     true,
}

// 创建存储客户端
store, err := cystore.NewStore(config)
if err != nil {
    // 处理错误
}
```

### 华为云 OBS 配置

```go
import (
    "github.com/fj1981/infrakit/pkg/cystore"
)

config := &cystore.Config{
    Provider:   cystore.ProviderHuaweiOBS,
    Region:     "cn-north-4",
    Secure:     true,
    Endpoint:   "obs.cn-north-4.myhuaweicloud.com",
    AccessKey:  "your-access-key",
    SecretKey:  "your-secret-key",
    UseSSL:     true,
    // 可选参数
    SessionToken: "your-security-token", // 如果需要使用临时凭证
}

// 创建存储客户端
store, err := cystore.NewStore(config)
if err != nil {
    // 处理错误
}
```

### 阿里云 OSS 配置

```go
import (
    "github.com/fj1981/infrakit/pkg/cystore"
)

config := &cystore.Config{
    Provider:   cystore.ProviderAliyunOSS,
    Region:     "oss-cn-hangzhou",
    Secure:     true,
    Endpoint:   "oss-cn-hangzhou.aliyuncs.com",
    AccessKey:  "your-access-key",
    SecretKey:  "your-secret-key",
    UseSSL:     true,
}

// 创建存储客户端
store, err := cystore.NewStore(config)
if err != nil {
    // 处理错误
}
```

### 本地文件存储配置

```go
import (
    "github.com/fj1981/infrakit/pkg/cystore"
)

config := &cystore.Config{
    Provider: cystore.ProviderLocal,
    BasePath: "/path/to/storage",
}

// 创建存储客户端
store, err := cystore.NewStore(config)
if err != nil {
    // 处理错误
}
```

## 使用示例

### 上传文件

```go
import (
    "context"
    "os"
    "path/filepath"
)

func uploadFile(store *cystore.Store, bucketName, filePath string) error {
    // 打开文件
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()

    // 获取文件信息
    fileInfo, err := file.Stat()
    if err != nil {
        return err
    }

    // 获取文件名
    fileName := filepath.Base(filePath)
    
    // 获取内容类型
    contentType := cystore.GetContentType(fileName)

    // 上传文件
    ctx := context.Background()
    _, err = store.Upload(ctx, bucketName, fileName, file, fileInfo.Size(), contentType)
    return err
}
```

### 下载文件

```go
func downloadFile(store *cystore.Store, bucketName, objectName, destPath string) error {
    ctx := context.Background()
    
    // 下载文件
    reader, _, err := store.Download(ctx, bucketName, objectName)
    if err != nil {
        return err
    }
    defer reader.Close()
    
    // 创建目标文件
    destFile, err := os.Create(destPath)
    if err != nil {
        return err
    }
    defer destFile.Close()
    
    // 复制内容
    _, err = io.Copy(destFile, reader)
    return err
}
```

### 生成预签名 URL

```go
func getDownloadURL(store *cystore.Store, bucketName, objectName string) (string, error) {
    ctx := context.Background()
    
    // 生成有效期为 1 小时的下载链接
    return store.GetURL(ctx, bucketName, objectName, 3600)
}

func getUploadURL(store *cystore.Store, bucketName, objectName string) (string, error) {
    ctx := context.Background()
    
    // 生成有效期为 1 小时的上传链接
    return store.PutURL(ctx, bucketName, objectName, 3600)
}
```

### 列出文件

```go
func listFiles(store *cystore.Store, bucketName, prefix string) ([]cystore.ObjectInfo, error) {
    ctx := context.Background()
    
    // 列出指定前缀的所有文件
    return store.ListFiles(ctx, bucketName, prefix)
}
```

### 删除文件

```go
func deleteFile(store *cystore.Store, bucketName, objectName string) error {
    ctx := context.Background()
    
    // 删除文件
    return store.Delete(ctx, bucketName, objectName)
}
```

## 高级用法

### 直接使用底层 Provider

如果需要使用底层 Provider 提供的特定功能，可以通过 `Provider()` 方法获取：

```go
// 获取底层 Provider
provider := store.Provider()

// 使用 Provider 特定的功能
ctx := context.Background()
buckets, err := provider.ListBuckets(ctx)
```

## 扩展

要添加新的存储提供商，只需实现 `Provider` 接口并在 `NewStore` 函数中添加相应的初始化逻辑。

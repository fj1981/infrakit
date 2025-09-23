package main

import (
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/fj1981/infrakit/pkg/cydb"
	_ "github.com/fj1981/infrakit/pkg/cydb/sql/mysql"
	_ "github.com/fj1981/infrakit/pkg/cydb/sql/postgresql"
	"github.com/fj1981/infrakit/pkg/cydist"
	"github.com/fj1981/infrakit/pkg/cygin"
	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyswag"
	"github.com/gin-gonic/gin"
)

// @title           用户管理 API
// @version         1.0
// @description     这是一个用户管理系统的API服务
// @termsOfService  http://swagger.io/terms/

// @contact.name   API Support
// @contact.url    http://www.example.com/support
// @contact.email  support@example.com

// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html

// @host      localhost:8080
// @BasePath  /api/v1

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

// 用户请求结构体
type UserRequest struct {
	Name     string `json:"name" binding:"required" example:"张三"`                 // 用户名
	Age      int    `json:"age" binding:"required,gt=0" example:"25"`             // 年龄
	Email    string `json:"email" binding:"email" example:"zhangsan@example.com"` // 邮箱
	UserType string `json:"user_type" header:"X-User-Type"`                       // 从Header获取
	// 可以添加更多字段
}

// 用户响应结构体
type UserResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Email   string `json:"email"`
	Message string `json:"message"`
}

// 复杂请求响应结构体
type ComplexResponse struct {
	UserID      string `json:"user_id" example:"user-123"`                                  // 用户ID
	DocID       string `json:"doc_id" example:"doc-456"`                                    // 文档ID
	Title       string `json:"title" example:"项目报告"`                                        // 文档标题
	DocumentURL string `json:"document_url" example:"https://example.com/files/doc123.pdf"` // 文档URL
	FileSize    int64  `json:"file_size" example:"1024000"`                                 // 文件大小（字节）
	Version     int    `json:"version" example:"1"`                                         // 文档版本
	Format      string `json:"format" example:"pdf"`                                        // 文档格式
	UploadTime  string `json:"upload_time" example:"2023-08-30T15:30:45Z"`                  // 上传时间
	Message     string `json:"message" example:"文档上传成功"`                                    // 响应消息
}

// ErrorResponse 错误响应结构体 (用于Swagger文档)
type ErrorResponse struct {
	Code    int    `json:"code" example:"101001"`                                // 错误代码
	Msg     string `json:"msg" example:"Invalid Parameter"`                      // 错误消息
	Status  int    `json:"status" example:"400"`                                 // HTTP状态码
	Details string `json:"details,omitempty" example:"Field 'name' is required"` // 错误详情
}

// 路径参数结构体
type UserPathParams struct {
	UserID string `uri:"user_id" binding:"required"` // 用户ID
}

// 复杂请求结构体 - 包含路径参数、查询参数、请求头和文件上传
type ComplexRequest struct {
	// 路径参数
	UserID string `uri:"user_id" binding:"required" example:"user-123"` // 用户ID
	DocID  string `uri:"doc_id" binding:"required" example:"doc-456"`   // 文档ID

	// 查询参数
	Version int    `form:"version" query:"version" binding:"omitempty,gte=1" example:"1"`             // 文档版本号
	Format  string `form:"format" query:"format" binding:"omitempty,oneof=pdf doc txt" example:"pdf"` // 文档格式

	// 请求头
	Token        string `header:"X-Auth-Token" binding:"required" example:"token123"` // 认证令牌
	ContentType  string `header:"Content-Type"`                                       // 内容类型
	ClientDevice string `header:"X-Client-Device" example:"mobile"`                   // 客户端设备类型

	// 表单数据
	Title       string `form:"title" binding:"required" example:"项目报告"` // 文档标题
	Description string `form:"description" example:"这是一份项目报告"`          // 文档描述
	Tags        string `form:"tags" example:"报告,项目,2023"`               // 标签，逗号分隔

	// 文件上传
	DocumentFile *multipart.FileHeader `form:"document" binding:"required"` // 主文档文件
	Attachment   *multipart.FileHeader `form:"attachment"`                  // 附件文件
}

// @Summary      创建用户
// @Description  创建一个新用户
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        user body UserRequest true "用户信息"
// @Param        X-User-Type header string false "用户类型" Enums(admin, regular)
// @Success      200  {object}  UserResponse
// @Failure      400  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /users [post]
// @Security     ApiKeyAuth
func createUser(c *gin.Context, req *UserRequest) (*UserResponse, error) {
	// 这里可以添加业务逻辑，如数据库操作等
	cylog.Info("Creating user", "name", req.Name, "age", req.Age, "email", req.Email, "type", req.UserType)

	// 模拟创建用户
	return &UserResponse{
		ID:      "user-123",
		Name:    req.Name,
		Age:     req.Age,
		Email:   req.Email,
		Message: "用户创建成功",
	}, nil
}

// @Summary      获取用户信息
// @Description  根据用户ID获取用户详细信息
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        user_id path string true "用户ID"
// @Success      200  {object}  UserResponse
// @Failure      400  {object}  ErrorResponse
// @Failure      404  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /users/{user_id} [get]
// @Security     ApiKeyAuth
func getUser(c *gin.Context, path *UserPathParams) (*UserResponse, error) {
	// 这里可以添加业务逻辑，如数据库查询等
	cylog.Info("Getting user", "user_id", path.UserID)

	// 模拟获取用户
	return &UserResponse{
		ID:      path.UserID,
		Name:    "张三",
		Age:     30,
		Email:   "zhangsan@example.com",
		Message: "获取用户成功",
	}, nil
}

// 更新用户信息
func updateUser(c *gin.Context, req *struct {
	UserPathParams
	UserRequest
}) (*UserResponse, error) {
	// 这里可以添加业务逻辑，如数据库更新等
	cylog.Info("Updating user", "user_id", req.UserID, "name", req.Name, "age", req.Age, "email", req.Email)

	// 模拟更新用户
	return &UserResponse{
		ID:      req.UserID,
		Name:    req.Name,
		Age:     req.Age,
		Email:   req.Email,
		Message: "用户更新成功",
	}, nil
}

// 删除用户
func deleteUser(c *gin.Context, path *UserPathParams) (*UserResponse, error) {
	// 这里可以添加业务逻辑，如数据库删除等
	cylog.Info("Deleting user", "user_id", path.UserID)

	// 模拟删除用户
	return &UserResponse{
		ID:      path.UserID,
		Message: "用户删除成功",
	}, nil
}

// 获取用户列表
func listUsers(c *gin.Context, req *struct{}) (*struct {
	Users   []UserResponse `json:"users"`
	Total   int            `json:"total"`
	Message string         `json:"message"`
}, error) {
	// 这里可以添加业务逻辑，如数据库查询等
	cylog.Info("Listing users")

	// 模拟用户列表
	users := []UserResponse{
		{
			ID:    "user-123",
			Name:  "张三",
			Age:   30,
			Email: "zhangsan@example.com",
		},
		{
			ID:    "user-456",
			Name:  "李四",
			Age:   28,
			Email: "lisi@example.com",
		},
		{
			ID:    "user-789",
			Name:  "王五",
			Age:   35,
			Email: "wangwu@example.com",
		},
	}

	return &struct {
		Users   []UserResponse `json:"users"`
		Total   int            `json:"total"`
		Message string         `json:"message"`
	}{
		Users:   users,
		Total:   len(users),
		Message: "获取用户列表成功",
	}, nil
}

// @Summary      上传用户文档
// @Description  上传用户文档，包含路径参数、查询参数、请求头和文件上传
// @Tags         documents
// @Accept       multipart/form-data
// @Produce      json
// @Param        user_id path string true "用户ID"
// @Param        doc_id path string true "文档ID"
// @Param        version query int false "文档版本号" minimum(1)
// @Param        format query string false "文档格式" Enums(pdf, doc, txt)
// @Param        X-Auth-Token header string true "认证令牌"
// @Param        X-Client-Device header string false "客户端设备类型"
// @Param        title formData string true "文档标题"
// @Param        description formData string false "文档描述"
// @Param        tags formData string false "标签，逗号分隔"
// @Param        document formData file true "主文档文件"
// @Param        attachment formData file false "附件文件"
// @Success      200  {object}  ComplexResponse
// @Failure      400  {object}  ErrorResponse
// @Failure      401  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /users/{user_id}/documents/{doc_id} [post]
// @Security     ApiKeyAuth
func uploadDocument(c *gin.Context, req *ComplexRequest) (*ComplexResponse, error) {
	// 记录请求信息
	cylog.Info("Uploading document",
		"user_id", req.UserID,
		"doc_id", req.DocID,
		"version", req.Version,
		"format", req.Format,
		"title", req.Title,
		"client_device", req.ClientDevice)

	// 验证认证令牌
	if req.Token == "" {
		return nil, cygin.NewError(401).WithDetail("缺少有效的认证令牌")
	}

	// 获取上传文件信息
	var documentSize int64
	var attachmentSize int64

	if req.DocumentFile != nil {
		documentSize = req.DocumentFile.Size
		cylog.Info("Document file info",
			"filename", req.DocumentFile.Filename,
			"size", documentSize,
			"content_type", req.DocumentFile.Header.Get("Content-Type"))

		// 这里可以实现文件保存逻辑
		// file, _ := req.DocumentFile.Open()
		// defer file.Close()
		// 处理文件内容...
	}

	if req.Attachment != nil {
		attachmentSize = req.Attachment.Size
		cylog.Info("Attachment file info",
			"filename", req.Attachment.Filename,
			"size", attachmentSize,
			"content_type", req.Attachment.Header.Get("Content-Type"))

		// 处理附件...
	}

	// 设置默认值
	version := req.Version
	if version == 0 {
		version = 1
	}

	format := req.Format
	if format == "" {
		format = "pdf"
	}

	// 返回响应
	return &ComplexResponse{
		UserID:      req.UserID,
		DocID:       req.DocID,
		Title:       req.Title,
		DocumentURL: fmt.Sprintf("https://example.com/files/%s/%s.%s", req.UserID, req.DocID, format),
		FileSize:    documentSize,
		Version:     version,
		Format:      format,
		UploadTime:  time.Now().UTC().Format(time.RFC3339),
		Message:     "文档上传成功",
	}, nil
}

// 定义API组
var api = func() cygin.ApiGroup {
	// 创建端点构建器
	eb := cygin.NewEndpointBuilder("/api/v1", "用户管理API", []string{"users"})

	// 构建所有端点
	return eb.Build(
		// 用户管理相关端点
		eb.POST("/users", createUser,
			cygin.WithSummary("创建用户"),
			cygin.WithDescription("创建一个新用户"),
		),
		eb.GET("/users/:user_id", getUser,
			cygin.WithSummary("获取用户信息"),
			cygin.WithDescription("根据用户ID获取用户详细信息"),
		),
		eb.PUT("/users/:user_id", updateUser,
			cygin.WithSummary("更新用户"),
			cygin.WithDescription("更新用户信息"),
		),
		eb.DELETE("/users/:user_id", deleteUser,
			cygin.WithSummary("删除用户"),
			cygin.WithDescription("删除指定用户"),
		),
		eb.GET("/users", listUsers,
			cygin.WithSummary("获取用户列表"),
			cygin.WithDescription("获取所有用户列表"),
		),

		// 文档管理相关端点
		eb.POST("/users/:user_id/documents/:doc_id", uploadDocument,
			cygin.WithSummary("上传用户文档"),
			cygin.WithDescription("上传用户文档，包含路径参数、查询参数、请求头和文件上传"),
			cygin.WithTags("documents"),
		),
	)
}()

type TestUser struct {
	ID int                 `json:"id"`
	AA map[string]struct{} ``
}

func main() {
	cydb.TestParseMySQL()
	tu := TestUser{ID: 1, AA: map[string]struct{}{"a": {}}}
	wrapper := cydist.NewCacheWrapper()
	wrapper.Set(context.Background(), "test", tu, cydist.WithTTL(time.Minute))
	var user TestUser
	wrapper.Get(context.Background(), "test", &user)
	fmt.Println(user)

	wrappedGetUser := cydist.CacheWrap1(
		wrapper,
		func(ctx context.Context, id int) (TestUser, error) {
			// This function should only be called on cache miss

			time.Sleep(5 * time.Second)
			return TestUser{ID: id, AA: map[string]struct{}{"a": {}}}, nil
		},
		cydist.WithTTL(5*time.Minute),
		cydist.WithKeyPrefix("testuser"),
		cydist.WithKey("users:User"),
	)
	tm := time.Now()
	user2, _ := wrappedGetUser(context.Background(), 1)
	fmt.Println("user2", user2)
	fmt.Println("user2 time", time.Since(tm))
	tm = time.Now()
	user3, _ := wrappedGetUser(context.Background(), 1)
	fmt.Println("user3", user3)
	fmt.Println("user3 time", time.Since(tm))
	tm = time.Now()
	wrapper.ResetCacheByBaseKey(context.Background(), "users")
	user4, _ := wrappedGetUser(context.Background(), 1)
	fmt.Println("user4", user4)
	fmt.Println("user4 time", time.Since(tm))

	// 创建服务器实例
	server := cygin.NewServer(
		cygin.WithMode(gin.DebugMode), // 开发模式
		cygin.WithCORS(),              // 启用CORS
		cygin.WithHealthCheck(),       // 健康检查
		cygin.WithVersionInfo(),       // 版本信息
		cygin.AddApiGroup(api),
		cygin.WithSwagger(
			cyswag.WithTitle("用户管理系统 API"),
			cyswag.WithVersion("1.0.0"),
			cyswag.WithHost("localhost:8180"),
			cyswag.WithScheme("http"),
		), // 启用Swagger文档
		cygin.WithPort(8180), // 设置端口
	)

	// 路由已通过 AddApiGroup(api) 设置

	// 添加一个简单的首页
	server.Engine.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Welcome to API Server",
			"status":  "running",
		})
	})

	// 启动服务器
	fmt.Println("Server starting on :8080")
	if err := server.Run(context.Background()); err != nil {
		cylog.Error("Server failed to start", "error", err)
	}
}

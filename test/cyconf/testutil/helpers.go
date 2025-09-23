package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// TestConfig 是所有测试用例共享的配置结构
type TestConfig struct {
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

// CreateTestConfigFile 创建测试配置文件
func CreateTestConfigFile(t *testing.T, path string, content string) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file %s: %v", path, err)
	}
	// 确保测试结束后删除文件
	t.Cleanup(func() {
		os.Remove(path)
	})
}

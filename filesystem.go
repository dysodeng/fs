package fs

import (
	"io"
	"os"
)

// FileSystem 文件系统接口
type FileSystem interface {
	// List 列出目录内容
	List(path string) ([]FileInfo, error)
	// MakeDir 创建目录
	MakeDir(path string, perm os.FileMode) error
	// RemoveDir 删除目录
	RemoveDir(path string) error

	// Create 创建文件并返回io.WriteCloser
	Create(path string) (io.WriteCloser, error)
	// CreateWithMetadata 创建文件并返回io.WriteCloser
	CreateWithMetadata(path string, metadata Metadata) (io.WriteCloser, error)
	// CreateWithOptions 创建文件并设置选项
	CreateWithOptions(path string, options CreateOptions) (io.WriteCloser, error)
	// Open 打开文件并返回io.ReadCloser
	Open(path string) (io.ReadCloser, error)
	// OpenFile 以指定模式打开文件
	OpenFile(path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error)
	// Remove 删除文件
	Remove(path string) error
	// Copy 复制文件
	Copy(src, dst string) error
	// Move 移动文件
	Move(src, dst string) error
	// Rename 重命名文件或目录
	Rename(oldPath, newPath string) error

	// Stat 获取文件/目录信息
	Stat(path string) (FileInfo, error)
	// SetMetadata 设置元数据
	SetMetadata(path string, metadata map[string]interface{}) error
	// GetMetadata 获取元数据
	GetMetadata(path string) (map[string]interface{}, error)

	// Exists 判断文件或目录是否存在
	Exists(path string) (bool, error)
	// IsDir 判断是否为目录
	IsDir(path string) (bool, error)
	// IsFile 判断是否为文件
	IsFile(path string) (bool, error)
}

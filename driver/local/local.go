package local

import (
	"io"
	"os"
	"path/filepath"

	"github.com/dysodeng/fs"
)

// local 本地文件系统
type local struct {
	rootPath string
}

func New(rootPath string) fs.FileSystem {
	return &local{
		rootPath: rootPath,
	}
}

// fullPath 获取完整路径
func (localFs *local) fullPath(path string) string {
	return filepath.Join(localFs.rootPath, path)
}

// List 列出目录内容
func (localFs *local) List(path string) ([]fs.FileInfo, error) {
	fullPath := localFs.fullPath(path)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	var files []fs.FileInfo
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, info)
	}
	return files, nil
}

// MakeDir 创建目录
func (localFs *local) MakeDir(path string, perm os.FileMode) error {
	return os.MkdirAll(localFs.fullPath(path), perm)
}

// RemoveDir 删除目录
func (localFs *local) RemoveDir(path string) error {
	return os.RemoveAll(localFs.fullPath(path))
}

// Create 创建文件并返回写入器
func (localFs *local) Create(path string) (io.WriteCloser, error) {
	return os.Create(localFs.fullPath(path))
}

// Open 打开文件并返回读取器
func (localFs *local) Open(path string) (io.ReadCloser, error) {
	return os.Open(localFs.fullPath(path))
}

// OpenFile 以指定模式打开文件
func (localFs *local) OpenFile(path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	return os.OpenFile(localFs.fullPath(path), flag, perm)
}

// Remove 删除文件
func (localFs *local) Remove(path string) error {
	return os.Remove(localFs.fullPath(path))
}

// Copy 复制文件
func (localFs *local) Copy(src, dst string) error {
	sourceFile, err := localFs.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := localFs.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// Move 移动文件
func (localFs *local) Move(src, dst string) error {
	return os.Rename(localFs.fullPath(src), localFs.fullPath(dst))
}

// Rename 重命名文件或目录
func (localFs *local) Rename(oldPath, newPath string) error {
	return os.Rename(localFs.fullPath(oldPath), localFs.fullPath(newPath))
}

// Stat 获取文件信息
func (localFs *local) Stat(path string) (fs.FileInfo, error) {
	return os.Stat(localFs.fullPath(path))
}

// SetMetadata 设置元数据（本地文件系统仅支持基本属性）
func (localFs *local) SetMetadata(path string, metadata map[string]interface{}) error {
	// 本地文件系统只支持修改文件权限和时间戳
	if mode, ok := metadata["mode"]; ok {
		if m, ok := mode.(os.FileMode); ok {
			if err := os.Chmod(localFs.fullPath(path), m); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetMetadata 获取元数据
func (localFs *local) GetMetadata(path string) (map[string]interface{}, error) {
	info, err := os.Stat(localFs.fullPath(path))
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"name":        info.Name(),
		"size":        info.Size(),
		"mode":        info.Mode(),
		"modify_time": info.ModTime(),
		"is_dir":      info.IsDir(),
	}, nil
}

// Preview 获取文件预览
func (localFs *local) Preview(path string) (io.ReadCloser, error) {
	return localFs.Open(path)
}

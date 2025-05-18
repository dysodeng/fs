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

func (localFs *local) MakeDir(path string, perm os.FileMode) error {
	return os.MkdirAll(localFs.fullPath(path), perm)
}

func (localFs *local) RemoveDir(path string) error {
	return os.RemoveAll(localFs.fullPath(path))
}

func (localFs *local) Create(path string) (io.WriteCloser, error) {
	return localFs.CreateWithOptions(path, fs.CreateOptions{})
}

func (localFs *local) CreateWithMetadata(path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return localFs.CreateWithOptions(path, fs.CreateOptions{Metadata: metadata})
}

func (localFs *local) CreateWithOptions(path string, options fs.CreateOptions) (io.WriteCloser, error) {
	file, err := os.Create(localFs.fullPath(path))
	if err != nil {
		return nil, err
	}

	// 本地文件系统不处理 ContentType，只处理 Metadata
	if options.Metadata != nil {
		if err = localFs.SetMetadata(path, options.Metadata); err != nil {
			file.Close()
			return nil, err
		}
	}

	return file, nil
}

func (localFs *local) Open(path string) (io.ReadCloser, error) {
	return os.Open(localFs.fullPath(path))
}

func (localFs *local) OpenFile(path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	return os.OpenFile(localFs.fullPath(path), flag, perm)
}

func (localFs *local) Remove(path string) error {
	return os.Remove(localFs.fullPath(path))
}

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

func (localFs *local) Move(src, dst string) error {
	return os.Rename(localFs.fullPath(src), localFs.fullPath(dst))
}

func (localFs *local) Rename(oldPath, newPath string) error {
	return os.Rename(localFs.fullPath(oldPath), localFs.fullPath(newPath))
}

func (localFs *local) Stat(path string) (fs.FileInfo, error) {
	return os.Stat(localFs.fullPath(path))
}

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

func (localFs *local) Exists(path string) (bool, error) {
	_, err := os.Stat(localFs.fullPath(path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (localFs *local) IsDir(path string) (bool, error) {
	info, err := os.Stat(localFs.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (localFs *local) IsFile(path string) (bool, error) {
	info, err := os.Stat(localFs.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

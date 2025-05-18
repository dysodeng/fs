package minio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dysodeng/fs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint        string // MinIO服务地址
	AccessKeyID     string // AccessKey
	SecretAccessKey string // SecretKey
	UseSSL          bool   // 是否使用SSL
	BucketName      string // 存储桶名称
	Location        string // 区域
}

// minioFs MinIO文件系统
type minioFs struct {
	client *minio.Client
	config Config
}

func New(config Config) (fs.FileSystem, error) {

	// 初始化MinIO客户端
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	// 确保bucket存在
	exists, err := client.BucketExists(context.Background(), config.BucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		err = client.MakeBucket(context.Background(), config.BucketName, minio.MakeBucketOptions{
			Region: config.Location,
		})
		if err != nil {
			return nil, err
		}
	}

	return &minioFs{
		client: client,
		config: config,
	}, nil
}

func (m *minioFs) List(path string) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo
	ctx := context.Background()

	// 使用ListObjects来获取指定前缀的对象
	opts := minio.ListObjectsOptions{
		Prefix:    strings.TrimRight(path, "/"),
		Recursive: false,
	}

	for object := range m.client.ListObjects(ctx, m.config.BucketName, opts) {
		if object.Err != nil {
			return nil, object.Err
		}
		fileInfos = append(fileInfos, newMinioFileInfo(object))
	}

	return fileInfos, nil
}

func (m *minioFs) MakeDir(path string, perm os.FileMode) error {
	// MinIO目录在写入文件时自动创建
	return nil
}

func (m *minioFs) RemoveDir(path string) error {
	ctx := context.Background()
	opts := minio.ListObjectsOptions{
		Prefix:    filepath.Clean(path) + "/",
		Recursive: true,
	}

	// 删除目录下的所有对象
	for object := range m.client.ListObjects(ctx, m.config.BucketName, opts) {
		err := m.client.RemoveObject(ctx, m.config.BucketName, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *minioFs) Create(path string) (io.WriteCloser, error) {
	return m.CreateWithOptions(path, fs.CreateOptions{})
}

func (m *minioFs) CreateWithMetadata(path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return m.CreateWithOptions(path, fs.CreateOptions{Metadata: metadata})
}

func (m *minioFs) CreateWithOptions(path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newMinioWriter(m.client, m.config.BucketName, path, options), nil
}

func (m *minioFs) Open(path string) (io.ReadCloser, error) {
	return m.client.GetObject(context.Background(), m.config.BucketName, path, minio.GetObjectOptions{})
}

func (m *minioFs) OpenFile(path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	// MinIO不支持追加模式，这里实现读写功能
	if flag&os.O_RDWR != 0 {
		return newMinioReadWriter(m.client, m.config.BucketName, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		// 对于只写模式，也返回 ReadWriter，但读取时会返回错误
		return newMinioReadWriter(m.client, m.config.BucketName, path), nil
	}
	// 对于只读模式，包装成 ReadWriteCloser
	reader, err := m.Open(path)
	if err != nil {
		return nil, err
	}
	return newMinioReadOnlyWrapper(reader), nil
}

func (m *minioFs) Remove(path string) error {
	return m.client.RemoveObject(context.Background(), m.config.BucketName, path, minio.RemoveObjectOptions{})
}

func (m *minioFs) Copy(src, dst string) error {
	_, err := m.client.CopyObject(context.Background(),
		minio.CopyDestOptions{
			Bucket: m.config.BucketName,
			Object: dst,
		},
		minio.CopySrcOptions{
			Bucket: m.config.BucketName,
			Object: src,
		})
	return err
}

func (m *minioFs) Move(src, dst string) error {
	// 先复制后删除来实现移动
	if err := m.Copy(src, dst); err != nil {
		return err
	}
	return m.Remove(src)
}

func (m *minioFs) Rename(oldPath, newPath string) error {
	return m.Move(oldPath, newPath)
}

func (m *minioFs) Stat(path string) (fs.FileInfo, error) {
	info, err := m.client.StatObject(context.Background(), m.config.BucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return newMinioFileInfo(info), nil
}

func (m *minioFs) SetMetadata(path string, metadata map[string]interface{}) error {
	// 将metadata转换为字符串map
	strMetadata := make(map[string]string)
	for k, v := range metadata {
		strMetadata[k] = fmt.Sprintf("%v", v)
	}

	// MinIO中需要通过复制对象来更新元数据
	_, err := m.client.CopyObject(context.Background(),
		minio.CopyDestOptions{
			Bucket:          m.config.BucketName,
			Object:          path + "_tmp",
			ReplaceMetadata: true,
			UserMetadata:    strMetadata,
		},
		minio.CopySrcOptions{
			Bucket: m.config.BucketName,
			Object: path,
		})
	if err != nil {
		return err
	}
	return m.Move(path+"_tmp", path)
}

func (m *minioFs) GetMetadata(path string) (map[string]interface{}, error) {
	info, err := m.client.StatObject(context.Background(), m.config.BucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range info.UserMetadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (m *minioFs) Exists(path string) (bool, error) {
	// 先检查是否为文件
	_, err := m.client.StatObject(context.Background(), m.config.BucketName, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}

	// 如果不是文件，检查是否为目录
	isDir, err := m.IsDir(path)
	if err != nil {
		return false, err
	}
	return isDir, nil
}

func (m *minioFs) IsDir(path string) (bool, error) {
	ctx := context.Background()
	opts := minio.ListObjectsOptions{
		Prefix:    strings.TrimRight(path, "/") + "/",
		Recursive: false,
		MaxKeys:   1,
	}

	for object := range m.client.ListObjects(ctx, m.config.BucketName, opts) {
		if object.Err != nil {
			return false, object.Err
		}
		return true, nil
	}
	return false, nil
}

func (m *minioFs) IsFile(path string) (bool, error) {
	_, err := m.client.StatObject(context.Background(), m.config.BucketName, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "The specified key does not exist.") {
		return false, nil
	}
	return false, err
}

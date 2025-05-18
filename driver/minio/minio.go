package minio

import (
	"context"
	"fmt"
	"io"
	"net/http"
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

func (m *minioFs) List(ctx context.Context, path string) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo

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

func (m *minioFs) MakeDir(ctx context.Context, path string, perm os.FileMode) error {
	// MinIO目录在写入文件时自动创建
	return nil
}

func (m *minioFs) RemoveDir(ctx context.Context, path string) error {
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

func (m *minioFs) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return m.CreateWithOptions(ctx, path, fs.CreateOptions{})
}

func (m *minioFs) CreateWithMetadata(ctx context.Context, path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return m.CreateWithOptions(ctx, path, fs.CreateOptions{Metadata: metadata})
}

func (m *minioFs) CreateWithOptions(ctx context.Context, path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newMinioWriter(ctx, m.client, m.config.BucketName, path, options), nil
}

func (m *minioFs) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, m.config.BucketName, path, minio.GetObjectOptions{})
}

func (m *minioFs) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	// MinIO不支持追加模式，这里实现读写功能
	if flag&os.O_RDWR != 0 {
		return newMinioReadWriter(ctx, m.client, m.config.BucketName, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		// 对于只写模式，也返回 ReadWriter，但读取时会返回错误
		return newMinioReadWriter(ctx, m.client, m.config.BucketName, path), nil
	}
	// 对于只读模式，包装成 ReadWriteCloser
	reader, err := m.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return newMinioReadOnlyWrapper(reader), nil
}

func (m *minioFs) Remove(ctx context.Context, path string) error {
	return m.client.RemoveObject(ctx, m.config.BucketName, path, minio.RemoveObjectOptions{})
}

func (m *minioFs) Copy(ctx context.Context, src, dst string) error {
	_, err := m.client.CopyObject(ctx,
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

func (m *minioFs) Move(ctx context.Context, src, dst string) error {
	// 先复制后删除来实现移动
	if err := m.Copy(ctx, src, dst); err != nil {
		return err
	}
	return m.Remove(ctx, src)
}

func (m *minioFs) Rename(ctx context.Context, oldPath, newPath string) error {
	return m.Move(ctx, oldPath, newPath)
}

func (m *minioFs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	info, err := m.client.StatObject(ctx, m.config.BucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return newMinioFileInfo(info), nil
}

func (m *minioFs) GetMimeType(ctx context.Context, path string) (string, error) {
	stat, err := m.client.StatObject(ctx, m.config.BucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return "", err
	}

	if stat.ContentType != "" {
		return stat.ContentType, nil
	}

	// 如果对象没有 ContentType，则读取文件内容进行检测
	obj, err := m.Open(ctx, path)
	if err != nil {
		return "", err
	}
	defer obj.Close()

	buffer := make([]byte, 512)
	_, err = obj.Read(buffer)
	if err != nil && err != io.EOF {
		return "", err
	}

	return http.DetectContentType(buffer), nil
}

func (m *minioFs) SetMetadata(ctx context.Context, path string, metadata map[string]interface{}) error {
	// 将metadata转换为字符串map
	strMetadata := make(map[string]string)
	for k, v := range metadata {
		strMetadata[k] = fmt.Sprintf("%v", v)
	}

	// MinIO中需要通过复制对象来更新元数据
	_, err := m.client.CopyObject(ctx,
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
	return m.Move(ctx, path+"_tmp", path)
}

func (m *minioFs) GetMetadata(ctx context.Context, path string) (map[string]interface{}, error) {
	info, err := m.client.StatObject(ctx, m.config.BucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range info.UserMetadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (m *minioFs) Exists(ctx context.Context, path string) (bool, error) {
	// 先检查是否为文件
	_, err := m.client.StatObject(ctx, m.config.BucketName, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}

	// 如果不是文件，检查是否为目录
	isDir, err := m.IsDir(ctx, path)
	if err != nil {
		return false, err
	}
	return isDir, nil
}

func (m *minioFs) IsDir(ctx context.Context, path string) (bool, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    strings.TrimRight(path, "/") + "/",
		Recursive: false,
		MaxKeys:   1,
	}

	objectChan := m.client.ListObjects(ctx, m.config.BucketName, opts)
	object, ok := <-objectChan
	if !ok {
		return false, nil
	}
	if object.Err != nil {
		return false, object.Err
	}
	return true, nil
}

func (m *minioFs) IsFile(ctx context.Context, path string) (bool, error) {
	_, err := m.client.StatObject(ctx, m.config.BucketName, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "The specified key does not exist.") {
		return false, nil
	}
	return false, err
}

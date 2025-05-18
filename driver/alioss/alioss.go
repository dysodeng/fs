package alioss

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dysodeng/fs"
)

type Config struct {
	Endpoint        string // OSS服务地址
	AccessKeyID     string // AccessKey
	SecretAccessKey string // SecretKey
	BucketName      string // 存储桶名称
}

// ossFs OSS文件系统
type ossFs struct {
	client *oss.Client
	bucket *oss.Bucket
	config Config
}

func New(config Config) (fs.FileSystem, error) {
	// 初始化OSS客户端
	client, err := oss.New(config.Endpoint, config.AccessKeyID, config.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	// 获取存储桶
	bucket, err := client.Bucket(config.BucketName)
	if err != nil {
		return nil, err
	}

	return &ossFs{
		client: client,
		bucket: bucket,
		config: config,
	}, nil
}

func (o *ossFs) List(path string) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo
	prefix := strings.TrimRight(path, "/")
	if prefix != "" {
		prefix += "/"
	}

	marker := ""
	for {
		lsRes, err := o.bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix), oss.Delimiter("/"))
		if err != nil {
			return nil, err
		}

		// 添加文件
		for _, object := range lsRes.Objects {
			fileInfos = append(fileInfos, newOssFileInfo(object))
		}

		// 添加目录
		for _, prefix := range lsRes.CommonPrefixes {
			fileInfos = append(fileInfos, newOssFileInfo(oss.ObjectProperties{
				Key: prefix,
			}))
		}

		if !lsRes.IsTruncated {
			break
		}
		marker = lsRes.NextMarker
	}

	return fileInfos, nil
}

func (o *ossFs) MakeDir(path string, perm os.FileMode) error {
	// OSS目录在写入文件时自动创建
	return nil
}

func (o *ossFs) RemoveDir(path string) error {
	prefix := strings.TrimRight(path, "/") + "/"
	marker := ""
	for {
		lsRes, err := o.bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix))
		if err != nil {
			return err
		}

		for _, object := range lsRes.Objects {
			err = o.bucket.DeleteObject(object.Key)
			if err != nil {
				return err
			}
		}

		if !lsRes.IsTruncated {
			break
		}
		marker = lsRes.NextMarker
	}
	return nil
}

func (o *ossFs) Create(path string) (io.WriteCloser, error) {
	return o.CreateWithOptions(path, fs.CreateOptions{})
}

func (o *ossFs) CreateWithMetadata(path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return o.CreateWithOptions(path, fs.CreateOptions{Metadata: metadata})
}

func (o *ossFs) CreateWithOptions(path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newOssWriter(o.bucket, path, options), nil
}

func (o *ossFs) Open(path string) (io.ReadCloser, error) {
	return o.bucket.GetObject(path)
}

func (o *ossFs) OpenFile(path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	if flag&os.O_RDWR != 0 {
		return newOssReadWriter(o.bucket, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		return newOssReadWriter(o.bucket, path), nil
	}
	reader, err := o.Open(path)
	if err != nil {
		return nil, err
	}
	return newOssReadOnlyWrapper(reader), nil
}

func (o *ossFs) Remove(path string) error {
	return o.bucket.DeleteObject(path)
}

func (o *ossFs) Copy(src, dst string) error {
	_, err := o.bucket.CopyObject(src, dst)
	return err
}

func (o *ossFs) Move(src, dst string) error {
	if err := o.Copy(src, dst); err != nil {
		return err
	}
	return o.Remove(src)
}

func (o *ossFs) Rename(oldPath, newPath string) error {
	return o.Move(oldPath, newPath)
}

func (o *ossFs) Stat(path string) (fs.FileInfo, error) {
	header, err := o.bucket.GetObjectMeta(path)
	if err != nil {
		return nil, err
	}

	lastModified, _ := time.ParseInLocation(time.RFC1123, header.Get("Last-Modified"), time.Local)
	fileSize, _ := strconv.ParseInt(header.Get("Content-Length"), 10, 64)

	return newOssFileInfo(oss.ObjectProperties{
		Key:          path,
		Size:         fileSize,
		LastModified: lastModified,
	}), nil
}

func (o *ossFs) GetMimeType(path string) (string, error) {
	header, err := o.bucket.GetObjectDetailedMeta(path)
	if err != nil {
		return "", err
	}

	contentType := header.Get("Content-Type")
	if contentType != "" {
		return contentType, nil
	}

	// 如果对象没有 Content-Type，则读取文件内容进行检测
	obj, err := o.Open(path)
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

func (o *ossFs) SetMetadata(path string, metadata map[string]interface{}) error {
	var opts []oss.Option
	for k, v := range metadata {
		opts = append(opts, oss.Meta(k, fmt.Sprintf("%v", v)))
	}

	// OSS中需要通过复制对象来更新元数据
	_, err := o.bucket.CopyObject(path, path, opts...)
	return err
}

func (o *ossFs) GetMetadata(path string) (map[string]interface{}, error) {
	header, err := o.bucket.GetObjectMeta(path)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range header {
		if strings.HasPrefix(k, "X-Oss-Meta-") {
			key := strings.TrimPrefix(k, "X-Oss-Meta-")
			metadata[key] = v[0]
		}
	}
	return metadata, nil
}

func (o *ossFs) Exists(path string) (bool, error) {
	exist, err := o.bucket.IsObjectExist(path)
	if err != nil {
		return false, err
	}
	if exist {
		return true, nil
	}

	// 如果不是文件，检查是否为目录
	return o.IsDir(path)
}

func (o *ossFs) IsDir(path string) (bool, error) {
	prefix := strings.TrimRight(path, "/") + "/"
	lsRes, err := o.bucket.ListObjects(oss.Prefix(prefix), oss.MaxKeys(1))
	if err != nil {
		return false, err
	}
	return len(lsRes.Objects) > 0 || len(lsRes.CommonPrefixes) > 0, nil
}

func (o *ossFs) IsFile(path string) (bool, error) {
	exist, err := o.bucket.IsObjectExist(path)
	if err != nil {
		return false, err
	}
	return exist, nil
}

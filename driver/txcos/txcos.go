package txcos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/dysodeng/fs"
	"github.com/tencentyun/cos-go-sdk-v5"
)

type Config struct {
	BucketURL  string        // 存储桶URL
	SecretID   string        // 密钥ID
	SecretKey  string        // 密钥Key
	AccessMode fs.AccessMode // 访问模式
}

// cosFs 腾讯云COS文件系统
type cosFs struct {
	client *cos.Client
	config Config
}

func New(config Config) (fs.FileSystem, error) {
	u, err := url.Parse(config.BucketURL)
	if err != nil {
		return nil, err
	}

	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  config.SecretID,
			SecretKey: config.SecretKey,
		},
	})

	return &cosFs{
		client: client,
		config: config,
	}, nil
}

func (driver *cosFs) List(ctx context.Context, path string, opts ...fs.Option) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo
	prefix := strings.TrimRight(path, "/")
	if prefix != "" {
		prefix += "/"
	}

	var marker string
	opt := &cos.BucketGetOptions{
		Prefix:    prefix,
		Delimiter: "/",
		Marker:    marker,
	}

	isTruncated := true
	for isTruncated {
		res, _, err := driver.client.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}

		// 添加文件
		for _, object := range res.Contents {
			fileInfos = append(fileInfos, newCosFileInfo(object))
		}

		// 添加目录
		for _, prefix := range res.CommonPrefixes {
			fileInfos = append(fileInfos, newCosFileInfo(cos.Object{
				Key: prefix,
			}))
		}

		isTruncated = res.IsTruncated
		marker = res.NextMarker
		opt.Marker = marker
	}

	return fileInfos, nil
}

func (driver *cosFs) MakeDir(_ context.Context, _ string, _ os.FileMode, opts ...fs.Option) error {
	// COS目录在写入文件时自动创建
	return nil
}

func (driver *cosFs) RemoveDir(ctx context.Context, path string, opts ...fs.Option) error {
	prefix := strings.TrimRight(path, "/") + "/"
	var marker string
	opt := &cos.BucketGetOptions{
		Prefix: prefix,
		Marker: marker,
	}

	isTruncated := true
	for isTruncated {
		res, _, err := driver.client.Bucket.Get(ctx, opt)
		if err != nil {
			return err
		}

		for _, object := range res.Contents {
			_, err = driver.client.Object.Delete(ctx, object.Key)
			if err != nil {
				return err
			}
		}

		isTruncated = res.IsTruncated
		marker = res.NextMarker
		opt.Marker = marker
	}
	return nil
}

func (driver *cosFs) Create(ctx context.Context, path string, opts ...fs.Option) (io.WriteCloser, error) {
	return newCosWriter(ctx, driver.client, path, opts...), nil
}

func (driver *cosFs) Open(ctx context.Context, path string, opts ...fs.Option) (io.ReadCloser, error) {
	resp, err := driver.client.Object.Get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (driver *cosFs) OpenFile(ctx context.Context, path string, flag int, _ os.FileMode, opts ...fs.Option) (io.ReadWriteCloser, error) {
	if flag&os.O_RDWR != 0 {
		return newCosReadWriter(ctx, driver.client, path, opts...), nil
	}
	if flag&os.O_WRONLY != 0 {
		return newCosReadWriter(ctx, driver.client, path, opts...), nil
	}
	reader, err := driver.Open(ctx, path, opts...)
	if err != nil {
		return nil, err
	}
	return newCosReadOnlyWrapper(reader), nil
}

func (driver *cosFs) Remove(ctx context.Context, path string, opts ...fs.Option) error {
	_, err := driver.client.Object.Delete(ctx, path)
	return err
}

func (driver *cosFs) Copy(ctx context.Context, src, dst string, opts ...fs.Option) error {
	sourceURL := strings.ReplaceAll(driver.config.BucketURL, "https://", "") + "/" + src
	_, _, err := driver.client.Object.Copy(ctx, dst, sourceURL, nil)
	return err
}

func (driver *cosFs) Move(ctx context.Context, src, dst string, opts ...fs.Option) error {
	if err := driver.Copy(ctx, src, dst); err != nil {
		return err
	}
	return driver.Remove(ctx, src)
}

func (driver *cosFs) Rename(ctx context.Context, oldPath, newPath string, opts ...fs.Option) error {
	return driver.Move(ctx, oldPath, newPath)
}

func (driver *cosFs) Stat(ctx context.Context, path string, opts ...fs.Option) (fs.FileInfo, error) {
	resp, err := driver.client.Object.Head(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	return newCosFileInfo(cos.Object{
		Key:          path,
		Size:         resp.ContentLength,
		LastModified: resp.Header.Get("Last-Modified"),
	}), nil
}

func (driver *cosFs) GetMimeType(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	resp, err := driver.client.Object.Head(ctx, path, nil)
	if err != nil {
		return "", err
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "" {
		return contentType, nil
	}

	// 如果对象没有Content-Type，则读取文件内容进行检测
	obj, err := driver.Open(ctx, path)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = obj.Close()
	}()

	buffer := make([]byte, 512)
	n, err := obj.Read(buffer)
	if err != nil && err != io.EOF {
		return "", err
	}

	return http.DetectContentType(buffer[:n]), nil
}

func (driver *cosFs) SetMetadata(ctx context.Context, path string, metadata map[string]any, opts ...fs.Option) error {
	opt := &cos.ObjectCopyOptions{
		ObjectCopyHeaderOptions: &cos.ObjectCopyHeaderOptions{
			XCosMetadataDirective: "Replaced",
		},
	}
	if metadata != nil {
		opt.XCosMetaXXX = &http.Header{}
		for k, v := range metadata {
			opt.XCosMetaXXX.Set(fmt.Sprintf("x-cos-meta-%s", k), fmt.Sprintf("%v", v))
		}
	}

	sourceURL := driver.config.BucketURL + "/" + path
	_, _, err := driver.client.Object.Copy(ctx, path+"_tmp", sourceURL, opt)
	if err != nil {
		return err
	}

	return driver.Move(ctx, path+"_tmp", path)
}

func (driver *cosFs) GetMetadata(ctx context.Context, path string, opts ...fs.Option) (map[string]any, error) {
	resp, err := driver.client.Object.Head(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range resp.Header {
		if strings.HasPrefix(k, "X-Cos-Meta-") {
			key := strings.TrimPrefix(k, "X-Cos-Meta-")
			if len(v) > 0 {
				metadata[key] = v[0]
			}
		}
		if strings.HasPrefix(k, "X-Cos-") && !strings.HasPrefix(k, "X-Cos-Meta-") {
			key := strings.TrimPrefix(k, "X-Cos-")
			if len(v) > 0 {
				metadata[key] = v[0]
			}
		}
	}

	return metadata, nil
}

func (driver *cosFs) Exists(ctx context.Context, path string, opts ...fs.Option) (bool, error) {
	if ok, err := driver.IsFile(ctx, path); err == nil && ok {
		return true, nil
	}
	return driver.IsDir(ctx, path)
}

func (driver *cosFs) IsDir(ctx context.Context, path string, opts ...fs.Option) (bool, error) {
	path = strings.TrimRight(path, "/") + "/"
	opt := &cos.BucketGetOptions{
		Prefix:    path,
		Delimiter: "/",
		MaxKeys:   1,
	}
	res, _, err := driver.client.Bucket.Get(ctx, opt)
	if err != nil {
		return false, err
	}
	return len(res.Contents) > 0 || len(res.CommonPrefixes) > 0, nil
}

func (driver *cosFs) IsFile(ctx context.Context, path string, opts ...fs.Option) (bool, error) {
	return driver.client.Object.IsExist(ctx, path)
}

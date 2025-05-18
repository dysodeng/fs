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
	BucketURL string // 存储桶URL
	SecretID  string // 密钥ID
	SecretKey string // 密钥Key
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

func (c *cosFs) List(ctx context.Context, path string) ([]fs.FileInfo, error) {
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
		res, _, err := c.client.Bucket.Get(ctx, opt)
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

func (c *cosFs) MakeDir(_ context.Context, _ string, _ os.FileMode) error {
	// COS目录在写入文件时自动创建
	return nil
}

func (c *cosFs) RemoveDir(ctx context.Context, path string) error {
	prefix := strings.TrimRight(path, "/") + "/"
	var marker string
	opt := &cos.BucketGetOptions{
		Prefix: prefix,
		Marker: marker,
	}

	isTruncated := true
	for isTruncated {
		res, _, err := c.client.Bucket.Get(ctx, opt)
		if err != nil {
			return err
		}

		for _, object := range res.Contents {
			_, err = c.client.Object.Delete(ctx, object.Key)
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

func (c *cosFs) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return c.CreateWithOptions(ctx, path, fs.CreateOptions{})
}

func (c *cosFs) CreateWithMetadata(ctx context.Context, path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return c.CreateWithOptions(ctx, path, fs.CreateOptions{Metadata: metadata})
}

func (c *cosFs) CreateWithOptions(ctx context.Context, path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newCosWriter(ctx, c.client, path, options), nil
}

func (c *cosFs) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := c.client.Object.Get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *cosFs) OpenFile(ctx context.Context, path string, flag int, _ os.FileMode) (io.ReadWriteCloser, error) {
	if flag&os.O_RDWR != 0 {
		return newCosReadWriter(ctx, c.client, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		return newCosReadWriter(ctx, c.client, path), nil
	}
	reader, err := c.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return newCosReadOnlyWrapper(reader), nil
}

func (c *cosFs) Remove(ctx context.Context, path string) error {
	_, err := c.client.Object.Delete(ctx, path)
	return err
}

func (c *cosFs) Copy(ctx context.Context, src, dst string) error {
	sourceURL := strings.Replace(c.config.BucketURL, "https://", "", -1) + "/" + src
	_, _, err := c.client.Object.Copy(ctx, dst, sourceURL, nil)
	return err
}

func (c *cosFs) Move(ctx context.Context, src, dst string) error {
	if err := c.Copy(ctx, src, dst); err != nil {
		return err
	}
	return c.Remove(ctx, src)
}

func (c *cosFs) Rename(ctx context.Context, oldPath, newPath string) error {
	return c.Move(ctx, oldPath, newPath)
}

func (c *cosFs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	resp, err := c.client.Object.Head(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	return newCosFileInfo(cos.Object{
		Key:          path,
		Size:         resp.ContentLength,
		LastModified: resp.Header.Get("Last-Modified"),
	}), nil
}

func (c *cosFs) GetMimeType(ctx context.Context, path string) (string, error) {
	resp, err := c.client.Object.Head(ctx, path, nil)
	if err != nil {
		return "", err
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "" {
		return contentType, nil
	}

	// 如果对象没有Content-Type，则读取文件内容进行检测
	obj, err := c.Open(ctx, path)
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

func (c *cosFs) SetMetadata(ctx context.Context, path string, metadata map[string]interface{}) error {
	opt := &cos.ObjectCopyOptions{
		ObjectCopyHeaderOptions: &cos.ObjectCopyHeaderOptions{
			XCosMetadataDirective: "Replaced",
		},
	}
	for k, v := range metadata {
		opt.ObjectCopyHeaderOptions.XCosMetaXXX = &http.Header{}
		opt.ObjectCopyHeaderOptions.XCosMetaXXX.Set(fmt.Sprintf("x-cos-meta-%s", k), fmt.Sprintf("%v", v))
	}

	sourceURL := c.config.BucketURL + "/" + path
	_, _, err := c.client.Object.Copy(ctx, path+"_tmp", sourceURL, opt)
	if err != nil {
		return err
	}

	return c.Move(ctx, path+"_tmp", path)
}

func (c *cosFs) GetMetadata(ctx context.Context, path string) (map[string]interface{}, error) {
	resp, err := c.client.Object.Head(ctx, path, nil)
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

func (c *cosFs) Exists(ctx context.Context, path string) (bool, error) {
	if ok, err := c.IsFile(ctx, path); err == nil && ok {
		return true, nil
	}
	return c.IsDir(ctx, path)
}

func (c *cosFs) IsDir(ctx context.Context, path string) (bool, error) {
	path = strings.TrimRight(path, "/") + "/"
	opt := &cos.BucketGetOptions{
		Prefix:    path,
		Delimiter: "/",
		MaxKeys:   1,
	}
	res, _, err := c.client.Bucket.Get(ctx, opt)
	if err != nil {
		return false, err
	}
	return len(res.Contents) > 0 || len(res.CommonPrefixes) > 0, nil
}

func (c *cosFs) IsFile(ctx context.Context, path string) (bool, error) {
	return c.client.Object.IsExist(ctx, path)
}

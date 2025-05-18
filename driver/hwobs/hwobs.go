package hwobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/dysodeng/fs"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type Config struct {
	Endpoint        string // OBS服务地址
	AccessKeyID     string // AccessKey
	SecretAccessKey string // SecretKey
	BucketName      string // 存储桶名称
}

// obsFs OBS文件系统
type obsFs struct {
	client *obs.ObsClient
	config Config
}

func New(config Config) (fs.FileSystem, error) {
	// 初始化OBS客户端
	client, err := obs.New(config.AccessKeyID, config.SecretAccessKey, config.Endpoint)
	if err != nil {
		return nil, err
	}

	return &obsFs{
		client: client,
		config: config,
	}, nil
}

func (o *obsFs) List(ctx context.Context, path string) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo
	prefix := strings.TrimRight(path, "/")
	if prefix != "" {
		prefix += "/"
	}

	marker := ""
	for {
		input := &obs.ListObjectsInput{
			Bucket: o.config.BucketName,
			Marker: marker,
		}
		input.Prefix = prefix

		output, err := o.client.ListObjects(input)
		if err != nil {
			return nil, err
		}

		// 添加文件
		for _, object := range output.Contents {
			fileInfos = append(fileInfos, newObsFileInfo(object))
		}

		// 添加目录
		for _, prefix := range output.CommonPrefixes {
			fileInfos = append(fileInfos, newObsFileInfo(obs.Content{
				Key: prefix,
			}))
		}

		if !output.IsTruncated {
			break
		}
		marker = output.NextMarker
	}

	return fileInfos, nil
}

func (o *obsFs) MakeDir(ctx context.Context, path string, perm os.FileMode) error {
	// OBS目录在写入文件时自动创建
	return nil
}

func (o *obsFs) RemoveDir(ctx context.Context, path string) error {
	prefix := strings.TrimRight(path, "/") + "/"
	marker := ""
	for {
		input := &obs.ListObjectsInput{
			Bucket: o.config.BucketName,
			Marker: marker,
		}
		input.Prefix = prefix

		output, err := o.client.ListObjects(input)
		if err != nil {
			return err
		}

		for _, object := range output.Contents {
			_, err = o.client.DeleteObject(&obs.DeleteObjectInput{
				Bucket: o.config.BucketName,
				Key:    object.Key,
			})
			if err != nil {
				return err
			}
		}

		if !output.IsTruncated {
			break
		}
		marker = output.NextMarker
	}
	return nil
}

func (o *obsFs) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return o.CreateWithOptions(ctx, path, fs.CreateOptions{})
}

func (o *obsFs) CreateWithMetadata(ctx context.Context, path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return o.CreateWithOptions(ctx, path, fs.CreateOptions{Metadata: metadata})
}

func (o *obsFs) CreateWithOptions(ctx context.Context, path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newObsWriter(ctx, o.client, o.config.BucketName, path, options), nil
}

func (o *obsFs) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	input := &obs.GetObjectInput{}
	input.Bucket = o.config.BucketName
	input.Key = path
	output, err := o.client.GetObject(input)
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (o *obsFs) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	if flag&os.O_RDWR != 0 {
		return newObsReadWriter(ctx, o.client, o.config.BucketName, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		return newObsReadWriter(ctx, o.client, o.config.BucketName, path), nil
	}
	reader, err := o.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return newObsReadOnlyWrapper(reader), nil
}

func (o *obsFs) Remove(ctx context.Context, path string) error {
	_, err := o.client.DeleteObject(&obs.DeleteObjectInput{
		Bucket: o.config.BucketName,
		Key:    path,
	})
	return err
}

func (o *obsFs) Copy(ctx context.Context, src, dst string) error {
	input := &obs.CopyObjectInput{}
	input.Bucket = o.config.BucketName
	input.Key = dst
	input.CopySourceBucket = o.config.BucketName
	input.CopySourceKey = src
	_, err := o.client.CopyObject(input)
	return err
}

func (o *obsFs) Move(ctx context.Context, src, dst string) error {
	if err := o.Copy(ctx, src, dst); err != nil {
		return err
	}
	return o.Remove(ctx, src)
}

func (o *obsFs) Rename(ctx context.Context, oldPath, newPath string) error {
	return o.Move(ctx, oldPath, newPath)
}

func (o *obsFs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: o.config.BucketName,
		Key:    path,
	}
	output, err := o.client.GetObjectMetadata(input)
	if err != nil {
		return nil, err
	}

	return newObsFileInfo(obs.Content{
		Key:          path,
		Size:         output.ContentLength,
		LastModified: output.LastModified,
	}), nil
}

func (o *obsFs) GetMimeType(ctx context.Context, path string) (string, error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: o.config.BucketName,
		Key:    path,
	}
	output, err := o.client.GetObjectMetadata(input)
	if err != nil {
		return "", err
	}

	if output.ContentType != "" {
		return output.ContentType, nil
	}

	// 如果对象没有 Content-Type，则读取文件内容进行检测
	obj, err := o.Open(ctx, path)
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

func (o *obsFs) SetMetadata(ctx context.Context, path string, metadata map[string]interface{}) error {
	input := &obs.CopyObjectInput{}
	input.Bucket = o.config.BucketName
	input.Key = path
	input.CopySourceBucket = o.config.BucketName
	input.CopySourceKey = path + "_tmp"

	input.Metadata = make(map[string]string)
	for k, v := range metadata {
		input.Metadata[k] = fmt.Sprintf("%v", v)
	}

	_, err := o.client.CopyObject(input)
	if err != nil {
		return err
	}

	return o.Move(ctx, path+"_tmp", path)
}

func (o *obsFs) GetMetadata(ctx context.Context, path string) (map[string]interface{}, error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: o.config.BucketName,
		Key:    path,
	}
	output, err := o.client.GetObjectMetadata(input)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range output.Metadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (o *obsFs) Exists(ctx context.Context, path string) (bool, error) {
	_, err := o.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: o.config.BucketName,
		Key:    path,
	})
	if err != nil {
		var obsErr obs.ObsError
		if errors.As(err, &obsErr) && obsErr.StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (o *obsFs) IsDir(ctx context.Context, path string) (bool, error) {
	path = strings.TrimRight(path, "/") + "/"
	input := &obs.ListObjectsInput{
		Bucket: o.config.BucketName,
	}
	input.Prefix = path
	input.Delimiter = "/"
	input.MaxKeys = 1
	output, err := o.client.ListObjects(input)
	if err != nil {
		return false, err
	}
	return len(output.Contents) > 0 || len(output.CommonPrefixes) > 0, nil
}

func (o *obsFs) IsFile(ctx context.Context, path string) (bool, error) {
	_, err := o.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: o.config.BucketName,
		Key:    path,
	})
	if err != nil {
		var obsErr obs.ObsError
		if errors.As(err, &obsErr) && obsErr.StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

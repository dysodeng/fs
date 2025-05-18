package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dysodeng/fs"
)

type Config struct {
	Region          string // AWS 区域
	Endpoint        string // S3 服务地址（可选，用于兼容其他 S3 协议的存储服务）
	AccessKeyID     string // AccessKey
	SecretAccessKey string // SecretKey
	BucketName      string // 存储桶名称
	UsePathStyle    bool   // 是否使用路径样式访问
}

// s3Fs S3文件系统
type s3Fs struct {
	client *s3.Client
	config Config
}

func New(conf Config) (fs.FileSystem, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(conf.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			conf.AccessKeyID,
			conf.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, err
	}

	// 如果指定了自定义endpoint，则使用自定义endpoint
	if conf.Endpoint != "" {
		cfg.BaseEndpoint = aws.String(conf.Endpoint)
	}

	client := s3.NewFromConfig(
		cfg,
		func(o *s3.Options) {
			o.UsePathStyle = conf.UsePathStyle
		},
	)

	return &s3Fs{
		client: client,
		config: conf,
	}, nil
}

func (s *s3Fs) List(ctx context.Context, path string) ([]fs.FileInfo, error) {
	var fileInfos []fs.FileInfo
	prefix := strings.TrimRight(path, "/")
	if prefix != "" {
		prefix += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.config.BucketName),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		// 添加文件
		for _, object := range output.Contents {
			fileInfos = append(fileInfos, newS3FileInfo(object))
		}

		// 添加目录
		for _, prefix := range output.CommonPrefixes {
			fileInfos = append(fileInfos, newS3FileInfo(types.Object{
				Key: prefix.Prefix,
			}))
		}
	}

	return fileInfos, nil
}

func (s *s3Fs) MakeDir(_ context.Context, _ string, _ os.FileMode) error {
	// S3目录在写入文件时自动创建
	return nil
}

func (s *s3Fs) RemoveDir(ctx context.Context, path string) error {
	prefix := strings.TrimRight(path, "/") + "/"

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.BucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, object := range output.Contents {
			_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.config.BucketName),
				Key:    object.Key,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *s3Fs) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return s.CreateWithOptions(ctx, path, fs.CreateOptions{})
}

func (s *s3Fs) CreateWithMetadata(ctx context.Context, path string, metadata fs.Metadata) (io.WriteCloser, error) {
	return s.CreateWithOptions(ctx, path, fs.CreateOptions{Metadata: metadata})
}

func (s *s3Fs) CreateWithOptions(ctx context.Context, path string, options fs.CreateOptions) (io.WriteCloser, error) {
	return newS3Writer(ctx, s.client, s.config.BucketName, path, options), nil
}

func (s *s3Fs) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Fs) OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
	if flag&os.O_RDWR != 0 {
		return newS3ReadWriter(ctx, s.client, s.config.BucketName, path), nil
	}
	if flag&os.O_WRONLY != 0 {
		return newS3ReadWriter(ctx, s.client, s.config.BucketName, path), nil
	}
	reader, err := s.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return newS3ReadOnlyWrapper(reader), nil
}

func (s *s3Fs) Remove(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	return err
}

func (s *s3Fs) Copy(ctx context.Context, src, dst string) error {
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.config.BucketName),
		Key:        aws.String(dst),
		CopySource: aws.String(fmt.Sprintf("%s/%s", s.config.BucketName, src)),
	})
	return err
}

func (s *s3Fs) Move(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Remove(ctx, src)
}

func (s *s3Fs) Rename(ctx context.Context, oldPath, newPath string) error {
	return s.Move(ctx, oldPath, newPath)
}

func (s *s3Fs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	return newS3FileInfo(types.Object{
		Key:          aws.String(path),
		Size:         output.ContentLength,
		LastModified: output.LastModified,
	}), nil
}

func (s *s3Fs) GetMimeType(ctx context.Context, path string) (string, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return "", err
	}

	if output.ContentType != nil {
		return *output.ContentType, nil
	}

	// 如果对象没有ContentType，则读取文件内容进行检测
	obj, err := s.Open(ctx, path)
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

func (s *s3Fs) SetMetadata(ctx context.Context, path string, metadata map[string]interface{}) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s.config.BucketName),
		Key:        aws.String(path + "_tmp"),
		CopySource: aws.String(fmt.Sprintf("%s/%s", s.config.BucketName, path)),
		Metadata:   make(map[string]string),
	}

	for k, v := range metadata {
		input.Metadata[k] = fmt.Sprintf("%v", v)
	}

	_, err := s.client.CopyObject(ctx, input)
	if err != nil {
		return err
	}

	return s.Move(ctx, path+"_tmp", path)
}

func (s *s3Fs) GetMetadata(ctx context.Context, path string) (map[string]interface{}, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})
	for k, v := range output.Metadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (s *s3Fs) Exists(ctx context.Context, path string) (bool, error) {
	if ok, err := s.IsFile(ctx, path); err == nil && ok {
		return true, nil
	}
	return s.IsDir(ctx, path)
}

func (s *s3Fs) IsDir(ctx context.Context, path string) (bool, error) {
	path = strings.TrimRight(path, "/") + "/"
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.config.BucketName),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1),
	}

	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return false, err
	}
	return len(output.Contents) > 0 || len(output.CommonPrefixes) > 0, nil
}

func (s *s3Fs) IsFile(ctx context.Context, path string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dysodeng/fs"
)

func (s *s3Fs) Uploader() fs.Uploader {
	return s
}

func (s *s3Fs) Upload(ctx context.Context, path string, reader io.Reader) error {
	file, err := s.Create(ctx, path)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

func (s *s3Fs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(path),
	}
	output, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", err
	}
	return *output.UploadId, nil
}

func (s *s3Fs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(s.config.BucketName),
		Key:        aws.String(path),
		PartNumber: aws.Int32(int32(partNumber)),
		UploadId:   aws.String(uploadID),
		Body:       data,
	}
	output, err := s.client.UploadPart(ctx, input)
	if err != nil {
		return "", err
	}
	return *output.ETag, nil
}

func (s *s3Fs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(part.PartNumber)),
			ETag:       aws.String(part.ETag),
		}
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.config.BucketName),
		Key:             aws.String(path),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	}
	_, err := s.client.CompleteMultipartUpload(ctx, input)
	return err
}

func (s *s3Fs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.config.BucketName),
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
	}
	_, err := s.client.AbortMultipartUpload(ctx, input)
	return err
}

func (s *s3Fs) ListMultipartUploads(ctx context.Context) ([]fs.MultipartUploadInfo, error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.config.BucketName),
	}

	result, err := s.client.ListMultipartUploads(ctx, input)
	if err != nil {
		return nil, err
	}

	uploads := make([]fs.MultipartUploadInfo, len(result.Uploads))
	for i, upload := range result.Uploads {
		uploads[i] = fs.MultipartUploadInfo{
			UploadID:   *upload.UploadId,
			Path:       *upload.Key,
			CreateTime: *upload.Initiated,
		}
	}
	return uploads, nil
}

func (s *s3Fs) ListUploadedParts(ctx context.Context, path string, uploadID string) ([]fs.MultipartPart, error) {
	input := &s3.ListPartsInput{
		Bucket:   aws.String(s.config.BucketName),
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
	}

	result, err := s.client.ListParts(ctx, input)
	if err != nil {
		return nil, err
	}

	parts := make([]fs.MultipartPart, len(result.Parts))
	for i, part := range result.Parts {
		parts[i] = fs.MultipartPart{
			PartNumber: int(*part.PartNumber),
			ETag:       *part.ETag,
			Size:       *part.Size,
		}
	}
	return parts, nil
}

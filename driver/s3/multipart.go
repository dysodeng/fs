package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dysodeng/fs"
)

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

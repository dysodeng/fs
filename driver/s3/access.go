package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dysodeng/fs"
)

func (driver *s3Fs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	var endpoint string
	if driver.config.UsePathStyle {
		endpoint = fmt.Sprintf("https://%s/%s", driver.config.Endpoint, driver.config.BucketName)
	} else {
		endpoint = fmt.Sprintf("https://%s.%s", driver.config.BucketName, driver.config.Endpoint)
	}
	cdnDomain := endpoint
	var useCdnDomain bool
	if o.CdnDomain != "" {
		cdnDomain = o.CdnDomain
		useCdnDomain = true
	}

	expires := 2 * time.Hour
	if o.SignUrlExpires > 0 {
		expires = o.SignUrlExpires
	}

	presignClient := s3.NewPresignClient(driver.client)
	signResult, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(driver.config.BucketName),
		Key:    aws.String(path),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", err
	}

	signUrl := strings.Replace(signResult.URL, "http://", "https://", -1)
	if useCdnDomain {
		signUrl = strings.Replace(signUrl, endpoint, cdnDomain, -1)
	}

	return signUrl, err
}

func (driver *s3Fs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	var endpoint string
	if driver.config.UsePathStyle {
		endpoint = fmt.Sprintf("https://%s/%s", driver.config.Endpoint, driver.config.BucketName)
	} else {
		endpoint = fmt.Sprintf("https://%s.%s", driver.config.BucketName, driver.config.Endpoint)
	}
	cdnDomain := endpoint
	var useCdnDomain bool
	if o.CdnDomain != "" {
		cdnDomain = o.CdnDomain
		useCdnDomain = true
	}

	var fullUrl string
	if driver.config.AccessMode == fs.Private {
		expires := 2 * time.Hour
		if o.SignUrlExpires > 0 {
			expires = o.SignUrlExpires
		}

		presignClient := s3.NewPresignClient(driver.client)
		signResult, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(driver.config.BucketName),
			Key:    aws.String(path),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = expires
		})
		if err != nil {
			return "", err
		}

		fullUrl = strings.Replace(signResult.URL, "http://", "https://", -1)
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.Replace(fullUrl, endpoint, cdnDomain, -1)
	}

	return fullUrl, nil
}

func (driver *s3Fs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", err
	}

	if driver.config.UsePathStyle {
		var originalPath = strings.TrimPrefix(u.Path, "/")
		originalPath = strings.Replace(originalPath, driver.config.BucketName, "", 1)
		return strings.TrimPrefix(originalPath, "/"), nil
	} else {
		return strings.TrimPrefix(u.Path, "/"), nil
	}
}

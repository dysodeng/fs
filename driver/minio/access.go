package minio

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dysodeng/fs"
)

func (driver *minioFs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	endpoint := fmt.Sprintf("https://%s/%s", driver.config.Endpoint, driver.config.BucketName)
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

	signUrl, err := driver.client.PresignedGetObject(ctx, driver.config.BucketName, path, expires, nil)
	if err != nil {
		return "", err
	}

	fullUrl := strings.Replace(signUrl.String(), "http://", "https://", -1)
	if useCdnDomain {
		fullUrl = strings.Replace(fullUrl, endpoint, cdnDomain, -1)
	}

	return fullUrl, nil
}

func (driver *minioFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	endpoint := fmt.Sprintf("https://%s/%s", driver.config.Endpoint, driver.config.BucketName)
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
		signUrl, err := driver.client.PresignedGetObject(ctx, driver.config.BucketName, path, expires, nil)
		if err != nil {
			return "", err
		}
		if driver.config.UseSSL {
			fullUrl = strings.Replace(signUrl.String(), "http://", "https://", -1)
		}
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.Replace(fullUrl, endpoint, cdnDomain, -1)
	}

	return fullUrl, nil
}

func (driver *minioFs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", err
	}

	var originalPath = strings.TrimPrefix(u.Path, "/")
	originalPath = strings.Replace(originalPath, driver.config.BucketName, "", 1)
	return strings.TrimPrefix(originalPath, "/"), nil
}

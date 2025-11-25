package alioss

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dysodeng/fs"
)

func (driver *ossFs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	path = driver.path(path)
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	endpoint := fmt.Sprintf("https://%s.%s", driver.config.BucketName, driver.config.Endpoint)
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

	signUrl, err := driver.bucket.SignURL(path, oss.HTTPGet, int64(expires.Seconds()), oss.WithContext(ctx))
	if err != nil {
		return "", err
	}

	signUrl = strings.ReplaceAll(signUrl, "http://", "https://")

	if useCdnDomain {
		signUrl = strings.ReplaceAll(signUrl, endpoint, cdnDomain)
	}

	return signUrl, err
}

func (driver *ossFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	path = driver.path(path)
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	endpoint := fmt.Sprintf("https://%s.%s", driver.config.BucketName, driver.config.Endpoint)
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

		var err error
		fullUrl, err = driver.bucket.SignURL(path, oss.HTTPGet, int64(expires.Seconds()), oss.WithContext(ctx))
		if err != nil {
			return "", err
		}
		fullUrl = strings.ReplaceAll(fullUrl, "http://", "https://")
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.ReplaceAll(fullUrl, endpoint, cdnDomain)
	}

	return fullUrl, nil
}

func (driver *ossFs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", nil
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

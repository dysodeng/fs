package txcos

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dysodeng/fs"
)

func (driver *cosFs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	cdnDomain := driver.config.BucketURL
	var useCdnDomain bool
	if o.CdnDomain != "" {
		cdnDomain = o.CdnDomain
		useCdnDomain = true
	}

	expires := 2 * time.Hour
	if o.SignUrlExpires > 0 {
		expires = o.SignUrlExpires
	}

	signUrlResult, err := driver.client.Object.GetPresignedURL2(ctx, "GET", path, expires, nil)
	if err != nil {
		return "", err
	}

	signUrl := strings.Replace(signUrlResult.String(), "http://", "https://", -1)
	if useCdnDomain {
		signUrl = strings.Replace(signUrl, driver.config.BucketURL, cdnDomain, -1)
	}

	return signUrl, err
}

func (driver *cosFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	cdnDomain := driver.config.BucketURL
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

		signUrlResult, err := driver.client.Object.GetPresignedURL2(ctx, "GET", path, expires, nil)
		if err != nil {
			return "", err
		}
		fullUrl = strings.Replace(signUrlResult.String(), "http://", "https://", -1)
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.Replace(fullUrl, driver.config.BucketURL, cdnDomain, -1)
	}

	return fullUrl, nil
}

func (driver *cosFs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", nil
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

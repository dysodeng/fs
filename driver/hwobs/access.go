package hwobs

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dysodeng/fs"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

func (driver *obsFs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
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

	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodGet,
		Bucket:  driver.config.BucketName,
		Key:     path,
		Expires: int(expires.Seconds()),
	}
	output, err := driver.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	signUrl := strings.ReplaceAll(strings.ReplaceAll(output.SignedUrl, "http://", "https://"), ":443", "")
	if useCdnDomain {
		signUrl = strings.ReplaceAll(signUrl, endpoint, cdnDomain)
	}

	return signUrl, err
}

func (driver *obsFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
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

		input := &obs.CreateSignedUrlInput{
			Method:  obs.HttpMethodGet,
			Bucket:  driver.config.BucketName,
			Key:     path,
			Expires: int(expires.Seconds()),
		}
		output, err := driver.client.CreateSignedUrl(input)
		if err != nil {
			return "", err
		}
		fullUrl = strings.ReplaceAll(strings.ReplaceAll(output.SignedUrl, "http://", "https://"), ":443", "")
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.ReplaceAll(fullUrl, endpoint, cdnDomain)
	}

	return fullUrl, nil
}

func (driver *obsFs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", nil
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

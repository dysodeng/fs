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

	signUrl := strings.Replace(strings.Replace(output.SignedUrl, "http://", "https://", -1), ":443", "", -1)
	if useCdnDomain {
		signUrl = strings.Replace(signUrl, endpoint, cdnDomain, -1)
	}

	return signUrl, err
}

func (driver *obsFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
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
		fullUrl = strings.Replace(strings.Replace(output.SignedUrl, "http://", "https://", -1), ":443", "", -1)
	} else {
		fullUrl = fmt.Sprintf("%s/%s", cdnDomain, path)
	}

	if useCdnDomain {
		fullUrl = strings.Replace(fullUrl, endpoint, cdnDomain, -1)
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

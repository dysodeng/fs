package local

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/dysodeng/fs"
)

func (driver *localFs) SignFullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	if o.CdnDomain != "" {
		return fmt.Sprintf("%s/%s", o.CdnDomain, path), nil
	}

	return path, nil
}

func (driver *localFs) FullUrl(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	if o.CdnDomain != "" {
		return fmt.Sprintf("%s/%s", o.CdnDomain, path), nil
	}

	return path, nil
}

func (driver *localFs) RelativePath(ctx context.Context, fullUrl string, opts ...fs.Option) (string, error) {
	u, err := url.Parse(fullUrl)
	if err != nil {
		return "", err
	}
	return strings.TrimLeft(u.Path, "/"), nil
}

package s3

import (
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3FileInfo 实现 fs.FileInfo 接口
type s3FileInfo struct {
	info types.Object
}

func newS3FileInfo(info types.Object) *s3FileInfo {
	return &s3FileInfo{info: info}
}

func (f *s3FileInfo) Name() string {
	if f.info.Key == nil {
		return ""
	}
	return *f.info.Key
}

func (f *s3FileInfo) Size() int64 {
	return *f.info.Size
}

func (f *s3FileInfo) Mode() os.FileMode {
	return 0644 // S3不支持文件权限，返回默认值
}

func (f *s3FileInfo) ModTime() time.Time {
	if f.info.LastModified == nil {
		return time.Time{}
	}
	return (*f.info.LastModified).Local()
}

func (f *s3FileInfo) IsDir() bool {
	return strings.HasSuffix(f.Name(), "/")
}

func (f *s3FileInfo) Sys() interface{} {
	return f.info
}

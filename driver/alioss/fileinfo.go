package alioss

import (
	"os"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossFileInfo 实现 fs.FileInfo 接口
type ossFileInfo struct {
	info oss.ObjectProperties
}

func newOssFileInfo(info oss.ObjectProperties) *ossFileInfo {
	return &ossFileInfo{info: info}
}

func (f *ossFileInfo) Name() string {
	return f.info.Key
}

func (f *ossFileInfo) Size() int64 {
	return f.info.Size
}

func (f *ossFileInfo) Mode() os.FileMode {
	return 0644 // OSS不支持文件权限，返回默认值
}

func (f *ossFileInfo) ModTime() time.Time {
	return f.info.LastModified
}

func (f *ossFileInfo) IsDir() bool {
	return strings.HasSuffix(f.info.Key, "/")
}

func (f *ossFileInfo) Sys() interface{} {
	return f.info
}

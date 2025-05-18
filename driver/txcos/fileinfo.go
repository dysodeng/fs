package txcos

import (
	"os"
	"strings"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
)

// cosFileInfo 实现 fs.FileInfo 接口
type cosFileInfo struct {
	info cos.Object
}

func newCosFileInfo(info cos.Object) *cosFileInfo {
	return &cosFileInfo{info: info}
}

func (f *cosFileInfo) Name() string {
	return f.info.Key
}

func (f *cosFileInfo) Size() int64 {
	return f.info.Size
}

func (f *cosFileInfo) Mode() os.FileMode {
	return 0644 // COS不支持文件权限，返回默认值
}

func (f *cosFileInfo) ModTime() time.Time {
	t, _ := time.Parse(time.RFC1123, f.info.LastModified)
	return t.Local()
}

func (f *cosFileInfo) IsDir() bool {
	return strings.HasSuffix(f.info.Key, "/")
}

func (f *cosFileInfo) Sys() interface{} {
	return f.info
}

package hwobs

import (
	"os"
	"strings"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// obsFileInfo 实现 fs.FileInfo 接口
type obsFileInfo struct {
	info obs.Content
}

func newObsFileInfo(info obs.Content) *obsFileInfo {
	return &obsFileInfo{info: info}
}

func (f *obsFileInfo) Name() string {
	return f.info.Key
}

func (f *obsFileInfo) Size() int64 {
	return f.info.Size
}

func (f *obsFileInfo) Mode() os.FileMode {
	return 0644 // OBS不支持文件权限，返回默认值
}

func (f *obsFileInfo) ModTime() time.Time {
	return f.info.LastModified.Local()
}

func (f *obsFileInfo) IsDir() bool {
	return strings.HasSuffix(f.info.Key, "/")
}

func (f *obsFileInfo) Sys() interface{} {
	return f.info
}

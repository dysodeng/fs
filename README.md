# Go FileSystem

[![Go Reference](https://pkg.go.dev/badge/github.com/dysodeng/fs.svg)](https://pkg.go.dev/github.com/dysodeng/fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/dysodeng/fs)](https://goreportcard.com/report/github.com/dysodeng/fs)
[![License](https://img.shields.io/github/license/dysodeng/fs.svg)](https://github.com/dysodeng/fs/blob/main/LICENSE)

Go FileSystem 是一个统一的文件系统接口实现，支持本地文件系统和多种云存储服务。它提供了一致的 API 来操作不同的存储系统，使得在不同存储系统之间切换变得简单。

## Features

- 统一的文件系统接口
- 支持多种存储驱动
  - 本地文件系统
  - MinIO 对象存储
  - 阿里云 OSS
  - 华为云 OBS
  - 腾讯云 COS
  - AWS S3
- 完整的文件操作支持
  - 文件的读写、复制、移动、删除
  - 目录的创建、删除、遍历
  - 文件元数据的读写
  - MIME 类型检测
  - 分片上传和断点续传
    - 支持大文件分片上传
    - 支持断点续传
    - 支持上传进度查询
    - 支持已上传分片管理

## Installation

```bash
go get github.com/dysodeng/fs
```

## Usage
### 本地文件系统
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/local"
)

func main() {
    fs := local.New("./storage")
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, World!"))
    writer.Close()
}
```
### MinIO 对象存储
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/minio"
)

func main() {
    config := minio.Config{
        Endpoint:        "play.min.io",
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
        UseSSL:          true,
        BucketName:      "your-bucket",
        Location:        "us-east-1",
    }
	
    fs, err := minio.New(config)
    if err != nil {
        panic(err)
    }
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, MinIO!"))
    writer.Close()
}
```

### 阿里云 OSS
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/alioss"
)

func main() {
    config := alioss.Config{
        Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
        BucketName:      "your-bucket",
    }
	
    fs, err := alioss.New(config)
    if err != nil {
        panic(err)
    }
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, OSS!"))
    writer.Close()
}
```

### 华为云 OBS
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/hwobs"
)

func main() {
    config := hwobs.Config{
        Endpoint:        "obs.cn-north-4.myhuaweicloud.com",
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
        BucketName:      "your-bucket",
    }
	
    fs, err := hwobs.New(config)
    if err != nil {
        panic(err)
    }
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, OBS!"))
    writer.Close()
}
```

### 腾讯云 COS
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/txcos"
)

func main() {
    config := txcos.Config{
        BucketURL:      "https://example-1234567890.cos.ap-guangzhou.myqcloud.com",
        SecretID:       "your-secret-id",
        SecretKey:      "your-secret-key",
    }
	
    fs, err := txcos.New(config)
    if err != nil {
        panic(err)
    }
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, COS!"))
    writer.Close()
}
```

### AWS S3
```go
package main

import (
    "context"
    "github.com/dysodeng/fs/driver/s3"
)

func main() {
    config := s3.Config{
        Region:          "us-east-1",
        Endpoint:        "https://s3.amazonaws.com", // S3 服务地址（可选，用于兼容其他 S3 协议的存储服务）
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
        BucketName:      "your-bucket",
        UsePathStyle:    false,                      // 是否使用路径样式访问
    }
    
    fs, err := s3.New(config)
    if err != nil {
        panic(err)
    }
    
    // 写入文件
    writer, err := fs.Create(context.Background(), "test.txt")
    if err != nil {
        panic(err)
    }
    writer.Write([]byte("Hello, S3!"))
    writer.Close()
}
```

## 分片上传和断点续传

所有存储驱动都支持分片上传和断点续传功能，可以用于处理大文件上传。

### 分片上传
分片上传是将大文件分割成多个小文件进行上传，每个小文件的大小可以根据实际情况进行调整。以下是一个示例：
```go
package main

import (
    "context"
    "io"
    "os"
    "github.com/dysodeng/fs"
    "github.com/dysodeng/fs/driver/alioss" // 这里以阿里云OSS为例
)

func main() {
    // 初始化存储驱动
    config := alioss.Config{
        Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
        BucketName:      "your-bucket",
    }
    
    fs, err := alioss.New(config)
    if err != nil {
        panic(err)
    }

    // 1. 初始化分片上传
    uploadID, err := fs.InitMultipartUpload(context.Background(), "large-file.zip")
    if err != nil {
        panic(err)
    }

    // 2. 分片上传
    var parts []fs.MultipartPart
    partSize := int64(5 * 1024 * 1024) // 5MB per part
    file, _ := os.Open("local-large-file.zip")
    
    for partNumber := 1; ; partNumber++ {
        buffer := make([]byte, partSize)
        n, err := file.Read(buffer)
        if err == io.EOF {
            break
        }
        
        part, err := fs.UploadPart(context.Background(), "large-file.zip", uploadID, partNumber, bytes.NewReader(buffer[:n]))
        if err != nil {
            // 出错时可以中断上传
            fs.AbortMultipartUpload(context.Background(), "large-file.zip", uploadID)
            panic(err)
        }
        
        parts = append(parts, part)
    }

    // 3. 完成上传
    err = fs.CompleteMultipartUpload(context.Background(), "large-file.zip", uploadID, parts)
    if err != nil {
        panic(err)
    }
}
```

### 断点续传
断点续传是在上传过程中，如果网络中断或上传失败，可以从上次中断的位置继续上传。以下是一个示例：
```go
func resumeUpload(fsCli fs.FileSystem, localFile, remotePath string) error {
    ctx := context.Background()
    
    // 1. 查找未完成的上传任务
    uploads, err := fsCli.ListMultipartUploads(ctx)
    if err != nil {
        return err
    }
    
    var targetUpload fs.MultipartUploadInfo
    for _, upload := range uploads {
        if upload.Path == remotePath {
            targetUpload = upload
            break
        }
    }
    
    // 2. 获取已上传的分片
    parts, err := fsCli.ListUploadedParts(ctx, remotePath, targetUpload.UploadID)
    if err != nil {
        return err
    }
    
    // 3. 创建已上传分片的映射
    uploadedParts := make(map[int]struct{})
    for _, part := range parts {
        uploadedParts[part.PartNumber] = struct{}{}
    }
    
    // 4. 继续上传未完成的分片
    file, err := os.Open(localFile)
    if err != nil {
        return err
    }
    defer file.Close()
    
    partSize := int64(5 * 1024 * 1024)
    for partNumber := 1; ; partNumber++ {
        // 跳过已上传的分片
        if _, exists := uploadedParts[partNumber]; exists {
            file.Seek(int64(partNumber-1)*partSize, io.SeekStart)
            continue
        }
        
        buffer := make([]byte, partSize)
        n, err := file.Read(buffer)
        if err == io.EOF {
            break
        }
        
        etag, err := fsCli.UploadPart(ctx, remotePath, targetUpload.UploadID, partNumber, bytes.NewReader(buffer[:n]))
        if err != nil {
            return err
        }
        
        parts = append(parts, fs.MultipartPart{
            PartNumber: partNumber,
            ETag:      etag,
        })
    }
    
    // 5. 完成上传
    return fsCli.CompleteMultipartUpload(ctx, remotePath, targetUpload.UploadID, parts)
}
```
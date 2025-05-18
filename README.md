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
- 完整的文件操作支持
  - 文件的读写、复制、移动、删除
  - 目录的创建、删除、遍历
  - 文件元数据的读写
  - MIME 类型检测

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
        UseSSL:         true,
        BucketName:     "your-bucket",
        Location:       "us-east-1",
    }
	
    fs, _ := minio.New(config)
    
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
        BucketName:     "your-bucket",
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
        BucketName:     "your-bucket",
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

package examples

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	f "github.com/dysodeng/fs"
	"github.com/dysodeng/fs/driver/hwobs"
)

func HwObs() {
	cnf := hwobs.Config{
		Endpoint:        "obs.cn-north-4.myhuaweicloud.com",
		AccessKeyID:     "your-access-key-id",
		SecretAccessKey: "your-access-key-secret",
		BucketName:      "your-bucket-name",
	}

	// 创建文件系统实例
	fs, err := hwobs.New(cnf)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建目录
	err = fs.MakeDir(ctx, "test", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// 创建并写入文件
	writer, err := fs.CreateWithOptions(ctx, "test/hello.txt", f.CreateOptions{
		ContentType: "text/plain",
		Metadata:    map[string]interface{}{"Author": "dysodeng", "Time": time.Now().Format(time.DateTime)},
	})
	if err != nil {
		log.Fatal(err)
	}
	content := []byte("Hello, OBS File System!")
	_, err = writer.Write(content)
	if err != nil {
		writer.Close()
		log.Fatal(err)
	}
	writer.Close()

	// 读取文件
	reader, err := fs.Open(ctx, "test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件内容: %s\n", string(data))

	// 复制文件
	err = fs.Copy(ctx, "test/hello.txt", "test/hello_copy.txt")
	if err != nil {
		log.Fatal(err)
	}

	// 列出目录内容
	files, err := fs.List(ctx, "test")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("目录内容:")
	for _, file := range files {
		fmt.Printf("- %s\n", file.Name())
	}

	// 文件信息
	info, err := fs.Stat(ctx, "test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("文件信息:")
	fmt.Printf("--->文件名: %s\n", info.Name())
	fmt.Printf("--->文件大小: %d\n", info.Size())
	fmt.Printf("--->文件权限: %s\n", info.Mode())
	fmt.Printf("--->文件修改时间: %s\n", info.ModTime().Format(time.DateTime))
	mimeType, err := fs.GetMimeType(ctx, "test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("--->文件MimeType: %s\n", mimeType)

	// 获取文件元数据
	metadata, err := fs.GetMetadata(ctx, "test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件元数据: %+v\n", metadata)

	// 删除文件
	err = fs.Remove(ctx, "test/hello_copy.txt")
	if err != nil {
		log.Fatal(err)
	}
}

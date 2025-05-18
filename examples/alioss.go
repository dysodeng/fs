package examples

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/dysodeng/fs/driver/alioss"
)

func AliOss() {
	cnf := alioss.Config{
		Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
		AccessKeyID:     "your-access-key-id",
		SecretAccessKey: "your-access-key-secret",
		BucketName:      "your-bucket-name",
	}
	// 创建文件系统实例
	fs, err := alioss.New(cnf)
	if err != nil {
		log.Fatal(err)
	}

	// 创建目录
	err = fs.MakeDir("test", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// 创建并写入文件
	writer, err := fs.Create("test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	content := []byte("Hello, OSS File System!")
	_, err = writer.Write(content)
	if err != nil {
		writer.Close()
		log.Fatal(err)
	}
	writer.Close()

	// 读取文件
	reader, err := fs.Open("test/hello.txt")
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
	err = fs.Copy("test/hello.txt", "test/hello_copy.txt")
	if err != nil {
		log.Fatal(err)
	}

	// 列出目录内容
	files, err := fs.List("test")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("目录内容:")
	for _, file := range files {
		fmt.Printf("- %s\n", file.Name())
	}

	// 文件信息
	info, err := fs.Stat("test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("文件信息:")
	fmt.Printf("--->文件名: %s\n", info.Name())
	fmt.Printf("--->文件大小: %d\n", info.Size())
	fmt.Printf("--->文件权限: %s\n", info.Mode())
	fmt.Printf("--->文件修改时间: %s\n", info.ModTime().Format(time.DateTime))
	mimeType, err := fs.GetMimeType("test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("--->文件MimeType: %s\n", mimeType)

	// 获取文件元数据
	metadata, err := fs.GetMetadata("test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件元数据: %+v\n", metadata)
}

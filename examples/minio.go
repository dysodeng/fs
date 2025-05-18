package examples

import (
	"fmt"
	"io"
	"log"
	"time"

	f "github.com/dysodeng/fs"
	"github.com/dysodeng/fs/driver/minio"
)

func MinIO() {
	// MinIO配置
	config := minio.Config{
		Endpoint:        "play.min.io", // MinIO服务地址
		AccessKeyID:     "your-access-key",
		SecretAccessKey: "your-secret-key",
		UseSSL:          true,
		BucketName:      "your-bucket",
		Location:        "us-east-1",
	}

	// 创建MinIO文件系统实例
	fs, err := minio.New(config)
	if err != nil {
		log.Fatal(err)
	}

	// 创建目录
	err = fs.MakeDir("test", 0755)
	if err != nil {
		log.Fatal("创建目录错误：" + err.Error())
	}

	// 写入文件
	writer, err := fs.CreateWithOptions("test/hello.txt", f.CreateOptions{
		Metadata:    map[string]interface{}{"Author": "dysodeng", "Time": time.Now().Format(time.DateTime)},
		ContentType: "text/plain",
	})
	if err != nil {
		log.Fatal(err)
	}
	content := []byte("Hello, MinIO!")
	_, err = writer.Write(content)
	if err != nil {
		log.Fatal("写入文件错误：" + err.Error())
	}
	writer.Close()

	// 读取文件
	reader, err := fs.Open("test/hello.txt")
	if err != nil {
		log.Fatal("读取文件错误：" + err.Error())
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal("读取文件内容错误：" + err.Error())
	}
	fmt.Printf("文件内容: %s\n", string(data))

	info, err := fs.Stat("test/hello.txt")
	if err != nil {
		log.Fatal("获取文件信息错误：" + err.Error())
	}
	fmt.Printf("文件信息: %+v\n", info)

	// 复制文件
	err = fs.Copy("test/hello.txt", "test/hello_copy.txt")
	if err != nil {
		log.Fatal(err)
	}

	// 列出目录内容
	files, err := fs.List("test/")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("目录内容:")
	for _, file := range files {
		fmt.Printf("- %s\n", file.Name())
	}

	// 获取文件元数据
	metadata, err := fs.GetMetadata("test/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件元数据: %+v\n", metadata)
}

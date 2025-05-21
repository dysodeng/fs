package examples

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/dysodeng/fs/driver/local"
)

func Local() {
	// 创建文件系统实例
	fs := local.New(local.Config{RootPath: "./tmp"})

	ctx := context.Background()

	// 创建目录
	err := fs.MakeDir(ctx, "local", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// 创建并写入文件
	writer, err := fs.Create(ctx, "local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	content := []byte("Hello, Local File System!")
	_, err = writer.Write(content)
	if err != nil {
		writer.Close()
		log.Fatal(err)
	}
	writer.Close()

	// 读取文件
	reader, err := fs.Open(ctx, "local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件内容: %s\n", string(data))

	// 使用 OpenFile 以追加模式打开文件
	file, err := fs.OpenFile(ctx, "local/hello.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	_, err = file.Write([]byte("\n追加的内容"))
	file.Close()
	if err != nil {
		log.Fatal(err)
	}

	// 复制文件
	err = fs.Copy(ctx, "local/hello.txt", "local/hello_copy.txt")
	if err != nil {
		log.Fatal(err)
	}

	// 列出目录内容
	files, err := fs.List(ctx, "/")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("目录内容:")
	for _, file := range files {
		fmt.Printf("- %s\n", file.Name())
	}

	// 文件信息
	info, err := fs.Stat(ctx, "local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("文件信息:")
	fmt.Printf("--->文件名: %s\n", info.Name())
	fmt.Printf("--->文件大小: %d\n", info.Size())
	fmt.Printf("--->文件权限: %s\n", info.Mode())
	fmt.Printf("--->文件修改时间: %s\n", info.ModTime().Format(time.DateTime))
	mimeType, err := fs.GetMimeType(ctx, "local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("--->文件MimeType: %s\n", mimeType)

	// 获取文件元数据
	metadata, err := fs.GetMetadata(ctx, "local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件元数据: %+v\n", metadata)
}

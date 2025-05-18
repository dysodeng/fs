package main

import (
	"fmt"
	"github.com/dysodeng/fs/driver/local"
	"io"
	"log"
	"os"
)

func main() {
	// 创建文件系统实例
	fs := local.New("./tmp")

	// 创建目录
	err := fs.MakeDir("local", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// 创建并写入文件
	writer, err := fs.Create("local/hello.txt")
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
	reader, err := fs.Open("local/hello.txt")
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
	file, err := fs.OpenFile("local/hello.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	_, err = file.Write([]byte("\n追加的内容"))
	file.Close()
	if err != nil {
		log.Fatal(err)
	}

	// 列出目录内容
	files, err := fs.List("/")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("目录内容:")
	for _, file := range files {
		fmt.Printf("- %s\n", file.Name())
	}

	// 获取文件元数据
	metadata, err := fs.GetMetadata("local/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("文件元数据: %+v\n", metadata)
}

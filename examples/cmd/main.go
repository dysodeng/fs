package main

import (
	"flag"

	"github.com/dysodeng/fs/examples"
)

var driver string

func main() {
	flag.StringVar(&driver, "driver", "local", "driver")
	flag.Parse()

	switch driver {
	case "local":
		examples.Local()
	case "minio":
		examples.MinIO()
	}
}

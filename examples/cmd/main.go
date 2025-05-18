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
	case "ali_oss":
		examples.AliOss()
	case "hw_obs":
		examples.HwObs()
	case "tx_cos":
		examples.TxCos()
	case "s3":
		examples.S3()
	}
}

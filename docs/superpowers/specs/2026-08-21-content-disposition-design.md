# Content-Disposition 上传属性设计

## 背景

当前 `fs.Options` 支持上传时指定 `ContentType`，但不能显式指定对象的
`Content-Disposition`。这会让需要浏览器内联展示的对象无法通过统一接口设置
`Content-Disposition: inline`。

## 目标

- 为公共 API 增加 `WithContentDisposition` option。
- 普通上传和分片上传初始化都向支持该属性的对象存储 SDK 透传原始值。
- 覆盖 MinIO、华为 OBS、AWS S3、阿里 OSS 和腾讯 COS。
- 保持未传 option 时的现有行为不变。

## 非目标

- 不解析、验证或改写 `Content-Disposition` 值。
- 不改变下载、签名 URL、复制或元数据更新行为。
- 不为本地文件系统持久化 HTTP 对象属性。
- 不引入新的跨 driver 对象属性抽象层。

## 公共 API

在 `fs.Options` 中增加：

```go
ContentDisposition string
```

增加 option：

```go
func WithContentDisposition(contentDisposition string) Option
```

调用方可以传入任意由后端 SDK 接受的值，例如 `inline` 或
`attachment; filename=file.pdf`。空字符串按“未设置”处理。

## Driver 映射

普通上传仍沿用 `Upload -> Create -> writer.Close` 的现有调用链。各 writer 保存
`Options.ContentDisposition`，并在发起上传请求时按下表设置 SDK 参数。

| Driver | 普通上传映射 | 分片初始化映射 |
| --- | --- | --- |
| MinIO | `minio.PutObjectOptions.ContentDisposition` | `minio.PutObjectOptions.ContentDisposition` |
| 华为 OBS | `obs.PutObjectInput.ContentDisposition` | `obs.InitiateMultipartUploadInput.ContentDisposition` |
| AWS S3 | `s3.PutObjectInput.ContentDisposition` | `s3.CreateMultipartUploadInput.ContentDisposition` |
| 阿里 OSS | `oss.ContentDisposition(...)` | `oss.ContentDisposition(...)` |
| 腾讯 COS | `cos.ObjectPutHeaderOptions.ContentDisposition` | `cos.InitiateMultipartUploadOptions.ContentDisposition` |

Local driver 不处理该 option，因为本地文件没有对象存储的 HTTP 响应属性。

## 错误与兼容性

库本身不校验 disposition 字符串；无效值产生的错误继续由对应 SDK 或服务端返回。
不传 `WithContentDisposition` 或传入空字符串时，不设置 SDK 字段，因此兼容现有调用方。
现有 `ContentType` 和 `Metadata` 映射保持不变。

## 测试

- 验证 `WithContentDisposition` 将原始值写入 `fs.Options`。
- 为五个云 driver 验证普通上传参数会收到 disposition 值。
- 为五个云 driver 验证分片初始化参数会收到 disposition 值。
- 验证空字符串不产生额外的 disposition 设置。
- 运行 `go test ./...`，确保所有包编译且回归测试通过。

测试遵循 TDD：先增加会因字段或映射缺失而失败的测试，确认失败原因，再添加最小实现。

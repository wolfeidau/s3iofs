# s3iofs

This package provides an S3 implementation for [Go1.16 filesystem interface](https://tip.golang.org/doc/go1.16#fs).

# Overview

This package provides an S3 implementation for the Go1.16 filesystem interface using the [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2).

The `S3FS` is currently read-only, and implements the following interfaces:

- `fs.FS`
- `fs.StatFS`
- `fs.ReadDirFS`

The `s3File` implements the following interfaces:

- `fs.FileInfo`
- `fs.DirEntry`
- `io.ReaderAt`
- `io.Seeker`

This enables libraries such as [apache arrow](https://arrow.apache.org/) to read parts of a parquet file from S3, without downloading the entire file.
# Usage 

```go
	// Load the Shared AWS Configuration (~/.aws/config) and enable request logging
	awscfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithClientLogMode(aws.LogRetries|aws.LogRequest),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)),
	)
	if err != nil {
		// ...
	}

	s3fs := s3iofs.New(os.Getenv("TEST_BUCKET_NAME"), awscfg)

	err = fs.WalkDir(s3fs, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			fmt.Println("dir:", path)
			return nil
		}
		fmt.Println("file:", path)
		return nil
	})
	if err != nil {
		// ...
	}
```

# Integration Tests

The integration tests for this package are in a separate module under the `integration`	directory, this to avoid polluting the main module with docker based testing dependencies used to run the tests locally against [minio](https://min.io/).

# Links

S3 implements ranges based on [HTTP Request ranges](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests), it well worth reading up on this if your using the `io.ReadSeek` interface.

# License

This application is released under Apache 2.0 license and is copyright [Mark Wolfe](https://www.wolfe.id.au).
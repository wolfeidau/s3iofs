# s3iofs

This package provides an S3 implementation for [Go1.16 filesystem interface](https://tip.golang.org/doc/go1.16#fs).

# Usage 

```go
	// Load the Shared AWS Configuration (~/.aws/config)
	awscfg, err := config.LoadDefaultConfig(context.TODO())
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

# License

This application is released under Apache 2.0 license and is copyright [Mark Wolfe](https://www.wolfe.id.au).
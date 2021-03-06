package s3iofs

import (
	"errors"
	"io/fs"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/net/context"
)

var (
	_ fs.FS        = (*S3FS)(nil)
	_ fs.StatFS    = (*S3FS)(nil)
	_ fs.ReadDirFS = (*S3FS)(nil)
)

// S3FS is a filesystem implementation using S3.
type S3FS struct {
	bucket   string
	s3client S3API
}

// New returns a new filesystem which provides access to the specified s3 bucket.
func New(bucket string, awscfg aws.Config) *S3FS {
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(awscfg)

	return &S3FS{
		s3client: client,
		bucket:   bucket,
	}
}

// Open opens the named file.
func (s3fs *S3FS) Open(name string) (fs.File, error) {

	if !fs.ValidPath(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrInvalid}
	}

	if name == "." {
		return &s3File{
			name:   name,
			bucket: s3fs.bucket,
			mode:   fs.ModeDir,
		}, nil
	}

	// optimistic GetObject using name
	res, err := s3fs.s3client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		var nfe *types.NotFound
		if errors.As(err, &nfe) {
			// fall back directory list
			return s3fs.openDirectory(name)
		}
		return nil, err
	}

	return &s3File{
		name:    name,
		bucket:  s3fs.bucket,
		res:     res,
		size:    res.ContentLength,
		modTime: aws.ToTime(res.LastModified),
	}, nil
}

// Stat returns a FileInfo describing the file.
func (s3fs *S3FS) Stat(name string) (fs.FileInfo, error) {

	f, err := s3fs.stat(name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	return f, nil
}

// ReadDir reads the named directory
func (s3fs *S3FS) ReadDir(name string) ([]fs.DirEntry, error) {

	f, err := s3fs.stat(name)
	if err != nil {
		return nil, err
	}

	if !f.IsDir() {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}

	prefix := name + "/"

	if name == "." {
		prefix = ""
	}

	list, err := s3fs.s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	entries := []fs.DirEntry{}

	// common prefixes are directories
	for _, commonPrefix := range list.CommonPrefixes {

		prefix := aws.ToString(commonPrefix.Prefix)

		dir := path.Base(prefix)

		// log.Info().Str("dir", dir).Msg("commonPrefix")

		entries = append(entries, &s3File{
			name:   dir,
			bucket: s3fs.bucket,
			mode:   fs.ModeDir,
		})
	}

	// contents are files
	for _, obj := range list.Contents {
		_, file := path.Split(aws.ToString(obj.Key))

		entries = append(entries, &s3File{
			name:    file,
			size:    obj.Size,
			modTime: aws.ToTime(obj.LastModified),
		})
	}

	// log.Info().Fields(map[string]interface{}{
	// 	"entries": entries,
	// }).Msg("ReadDir")

	return entries, nil
}

func (s3fs *S3FS) stat(name string) (fs.FileInfo, error) {

	if name == "." {
		return &s3File{
			name:   name,
			bucket: s3fs.bucket,
			mode:   fs.ModeDir,
		}, nil
	}

	list, err := s3fs.s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.bucket),
		Prefix:    aws.String(name),
		Delimiter: aws.String("/"),
		MaxKeys:   1,
	})
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}

	if len(list.CommonPrefixes) > 0 &&
		aws.ToString(list.CommonPrefixes[0].Prefix) == name+"/" {

		return &s3File{
			name:   name,
			bucket: s3fs.bucket,
			mode:   fs.ModeDir,
		}, nil
	}

	if len(list.Contents) > 0 &&
		aws.ToString(list.Contents[0].Key) == name {
		return &s3File{
			name:    name,
			bucket:  s3fs.bucket,
			size:    list.Contents[0].Size,
			modTime: aws.ToTime(list.Contents[0].LastModified),
		}, nil
	}

	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

func (s3fs *S3FS) openDirectory(name string) (fs.File, error) {
	f, err := s3fs.stat(name)
	if err != nil {
		return nil, err
	}

	if f.IsDir() {
		return f.(fs.File), nil
	}

	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

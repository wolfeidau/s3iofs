package s3iofs

import (
	"bytes"
	"errors"
	"io/fs"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/net/context"
)

var (
	_ fs.FS        = (*S3FS)(nil)
	_ fs.StatFS    = (*S3FS)(nil)
	_ fs.ReadDirFS = (*S3FS)(nil)
	_ RemoveFS     = (*S3FS)(nil)
	_ WriteFileFS  = (*S3FS)(nil)
)

// RemoveFS extend the fs.FS interface to add the Remove method.
type RemoveFS interface {
	fs.FS
	Remove(name string) error
}

// WriteFileFS extend the fs.FS interface to add the WriteFile method.
type WriteFileFS interface {
	fs.FS
	WriteFile(name string, data []byte, perm os.FileMode) error
}

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

// NewWithClient returns a new filesystem which provides access to the specified s3 bucket.
func NewWithClient(bucket string, client S3API) *S3FS {
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
			s3client: s3fs.s3client,
			name:     name,
			bucket:   s3fs.bucket,
			mode:     fs.ModeDir,
		}, nil
	}

	req := &s3.GetObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
	}

	// optimistic GetObject, with the body setup as the default stream used for reading
	// the goal here is to avoid subsequent get object calls triggered by small reads as observed
	// when testing with files larger than 3-5 kilobytes
	res, err := s3fs.s3client.GetObject(context.TODO(), req)
	if err != nil {
		var nfe *types.NotFound
		if errors.As(err, &nfe) {
			// fall back directory list
			return s3fs.openDirectory(name)
		}
		return nil, err
	}

	return &s3File{
		s3client: s3fs.s3client,
		name:     name,
		bucket:   s3fs.bucket,
		size:     aws.ToInt64(res.ContentLength),
		modTime:  aws.ToTime(res.LastModified),
		body:     res.Body,
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

// ReadDir reads the named directory.
func (s3fs *S3FS) ReadDir(name string) ([]fs.DirEntry, error) {
	f, err := s3fs.stat(name)
	if err != nil {
		return nil, err
	}

	if !f.IsDir() {
		return nil, &fs.PathError{Op: opRead, Path: name, Err: fs.ErrNotExist}
	}

	prefix, err := url.JoinPath(name, "/")
	if err != nil {
		return nil, err
	}

	if name == "." {
		prefix = ""
	}

	listRes, err := s3fs.s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	return listResToEntries(s3fs.bucket, s3fs.s3client, listRes)
}

// Remove removes the named file or directory.
//
// Note if the file doesn't exist in the s3 bucket, Remove returns nil.
func (s3fs *S3FS) Remove(name string) error {
	if name == "." {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}
	if name == "" {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}

	_, err := s3fs.s3client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	return nil
}

// WriteFile writes the data to the named file in s3.
//
// Note:
//   - If the file exists, WriteFile overwrites it.
//   - The provided mode is unused by this implementation.
func (s3fs *S3FS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if name == "." {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrInvalid}
	}
	if name == "" {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrInvalid}
	}

	_, err := s3fs.s3client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
		Body:   bytes.NewReader(data),
	})

	if err != nil {
		return &fs.PathError{Op: "write", Path: name, Err: err}
	}

	return nil
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
		MaxKeys:   aws.Int32(1),
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
			size:    aws.ToInt64(list.Contents[0].Size),
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

func listResToEntries(bucket string, s3client S3API, listRes *s3.ListObjectsV2Output) ([]fs.DirEntry, error) {
	entries := []fs.DirEntry{}

	// common prefixes are directories
	for _, commonPrefix := range listRes.CommonPrefixes {

		prefix := aws.ToString(commonPrefix.Prefix)

		// dir := path.Base(prefix)

		entries = append(entries, &s3File{
			s3client: s3client,
			name:     prefix,
			bucket:   bucket,
			mode:     fs.ModeDir,
		})
	}

	// contents are files
	for _, obj := range listRes.Contents {
		// _, file := path.Split(aws.ToString(obj.Key))

		entries = append(entries, &s3File{
			s3client: s3client,
			name:     aws.ToString(obj.Key),
			bucket:   bucket,
			size:     aws.ToInt64(obj.Size),
			modTime:  aws.ToTime(obj.LastModified),
		})
	}

	return entries, nil
}

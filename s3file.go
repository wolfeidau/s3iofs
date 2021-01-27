package s3iofs

import (
	"errors"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.FileInfo = (*s3File)(nil)
	_ fs.DirEntry = (*s3File)(nil)
)

type s3File struct {
	name    string
	bucket  string
	res     *s3.GetObjectOutput
	size    int64
	mode    fs.FileMode
	modTime time.Time // zero value for directories
}

func (s3f *s3File) Stat() (fs.FileInfo, error) {
	return s3f, nil
}

func (s3f *s3File) Info() (fs.FileInfo, error) {
	return s3f, nil
}

func (s3f *s3File) Read(p []byte) (int, error) {
	if s3f.IsDir() {
		return 0, &fs.PathError{Op: "read", Path: s3f.name, Err: errors.New("is a directory")}
	}
	return s3f.res.Body.Read(p)
}

func (s3f *s3File) Close() error {
	if s3f.IsDir() {
		return nil // NOOP for directories
	}
	return s3f.res.Body.Close()
}

// Name returns the name of the file (or subdirectory) described by the entry.
func (s3f *s3File) Name() string {
	return s3f.name
}

// Size length in bytes for regular files; system-dependent for others
func (s3f *s3File) Size() int64 {
	return s3f.size
}

// Mode file mode bits
func (s3f *s3File) Mode() fs.FileMode {
	return s3f.mode
}

// file mode bits
func (s3f *s3File) Type() fs.FileMode {
	return s3f.mode
}

// modification time
func (s3f *s3File) ModTime() time.Time {
	return s3f.modTime
}

// abbreviation for Mode().IsDir()
func (s3f *s3File) IsDir() bool {
	return s3f.Mode().IsDir()
}

// underlying data source (can return nil)
func (s3f *s3File) Sys() interface{} {
	return nil
}

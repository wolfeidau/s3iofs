package s3iofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.FileInfo    = (*s3File)(nil)
	_ fs.DirEntry    = (*s3File)(nil)
	_ io.ReaderAt    = (*s3File)(nil)
	_ io.Seeker      = (*s3File)(nil)
	_ fs.ReadDirFile = (*s3File)(nil)
)

const (
	opRead = "read"
	opSeek = "seek"
)

type s3File struct {
	s3client     S3API
	name         string
	bucket       string
	size         int64
	mode         fs.FileMode
	modTime      time.Time // zero value for directories
	offset       int64
	lastDirEntry string
	mutex        sync.Mutex
	body         io.ReadCloser
}

func (s3f *s3File) Stat() (fs.FileInfo, error) {
	return s3f, nil
}

func (s3f *s3File) Info() (fs.FileInfo, error) {
	return s3f, nil
}

func (s3f *s3File) Read(p []byte) (int, error) {
	if s3f.IsDir() {
		return 0, &fs.PathError{Op: opRead, Path: s3f.name, Err: errors.New("is a directory")}
	}

	if s3f.offset >= s3f.size {
		return 0, io.EOF
	}

	s3f.mutex.Lock()
	defer s3f.mutex.Unlock()
	if s3f.body != nil {
		n, err := s3f.body.Read(p)
		s3f.offset += int64(n) // update the current offset
		return n, err
	}

	// random access to S3 object is currently being used to read the file
	n, err := s3f.ReadAt(p, s3f.offset)
	s3f.offset += int64(n) // update the current offset
	if err != nil {
		// if we get an unexpected EOF, and we are at the end of the underlying file, return EOF as that is
		// the expected behavior
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// if we are at the end of the underlying file, return EOF as that is the expected behavior
			if s3f.offset == s3f.size {
				return n, io.EOF
			}
		}

		return n, err
	}

	return n, nil
}

func (s3f *s3File) ReadAt(p []byte, offset int64) (n int, err error) {
	ctx := context.Background()

	r, err := s3f.readerAt(ctx, offset, int64(len(p)))
	if err != nil {
		return 0, err
	}

	// ensure the buffer is read, or EOF is reached for this read of this "chunk"
	// given we are using offsets to read this block it is constrained by size of `p`
	size, err := io.ReadFull(r, p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return size, err
		}
		// check if we are at the end of the underlying file
		if offset+int64(size) > s3f.size {
			return size, err
		}
	}

	return size, r.Close()
}

func (s3f *s3File) Seek(offset int64, whence int) (int64, error) {
	// given the body stream doesn't support seek we will need to re-open the stream
	// using read at the new offset
	s3f.mutex.Lock()
	defer s3f.mutex.Unlock()
	if s3f.body != nil {
		err := s3f.body.Close()
		if err != nil {
			return 0, err
		}
		s3f.body = nil
	}

	switch whence {
	default:
		return 0, &fs.PathError{Op: opSeek, Path: s3f.name, Err: fs.ErrInvalid}
	case io.SeekStart:
		// offset += 0
	case io.SeekCurrent:
		offset += s3f.offset
	case io.SeekEnd:
		offset += s3f.size
	}
	if offset < 0 || offset > s3f.size {
		return 0, &fs.PathError{Op: opSeek, Path: s3f.name, Err: fs.ErrInvalid}
	}
	s3f.offset = offset

	return offset, nil
}

func (s3f *s3File) ReadDir(n int) ([]fs.DirEntry, error) {
	if !s3f.IsDir() {
		return nil, &fs.PathError{Op: opRead, Path: s3f.Name(), Err: fs.ErrNotExist}
	}

	prefix := s3f.name

	if s3f.name == "." {
		prefix = ""
	}

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3f.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	if n > 0 {
		params.MaxKeys = aws.Int32(int32(n))
	}

	if s3f.lastDirEntry != "" {
		params.StartAfter = aws.String(s3f.lastDirEntry)
	}

	listRes, err := s3f.s3client.ListObjectsV2(context.Background(), params)
	if err != nil {
		return nil, err
	}

	entries, err := listResToEntries(s3f.bucket, s3f.s3client, listRes)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, io.EOF
	}

	if n > 0 && len(entries) == n {
		des3f, ok := entries[n-1].(*s3File)
		if ok {
			s3f.lastDirEntry = des3f.name
		}
	}

	return entries, nil
}

func (s3f *s3File) readerAt(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	byteRange := buildRange(offset, length)

	req := &s3.GetObjectInput{
		Bucket: aws.String(s3f.bucket),
		Key:    aws.String(s3f.name),
		Range:  byteRange,
	}

	res, err := s3f.s3client.GetObject(ctx, req)
	if err != nil {
		return nil, &fs.PathError{Op: opRead, Path: s3f.name, Err: err}
	}

	return res.Body, nil
}

func (s3f *s3File) Close() error {
	s3f.mutex.Lock()
	defer s3f.mutex.Unlock()
	if s3f.body != nil {
		err := s3f.body.Close()
		if err != nil {
			return err
		}
		s3f.body = nil
	}

	return nil
}

// Name returns the name of the file (or subdirectory) described by the entry.
func (s3f *s3File) Name() string {
	return path.Base(s3f.name)
}

// Size length in bytes for regular files; system-dependent for others.
func (s3f *s3File) Size() int64 {
	return s3f.size
}

// Mode file mode bits.
func (s3f *s3File) Mode() fs.FileMode {
	return s3f.mode
}

// file mode bits.
func (s3f *s3File) Type() fs.FileMode {
	return s3f.mode
}

// modification time.
func (s3f *s3File) ModTime() time.Time {
	return s3f.modTime
}

// abbreviation for Mode().IsDir().
func (s3f *s3File) IsDir() bool {
	return s3f.Mode().IsDir()
}

// underlying data source (can return nil).
func (s3f *s3File) Sys() interface{} {
	return nil
}

func buildRange(offset, length int64) *string {
	switch {
	case offset > 0 && length < 0:
		return aws.String(fmt.Sprintf("bytes=%d-", offset))
	case length == 0:
		// AWS doesn't support a zero-length read; we'll read 1 byte and then
		// ignore it in favor of http.NoBody below.
		return aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+1))
	case length >= 0:
		return aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}

	return nil
}

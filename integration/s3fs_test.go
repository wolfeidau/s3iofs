package integration

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/s3iofs"
)

var (
	oneMegabyte    = 1024 * 1024
	twoMegabytes   = 1024 * 1024 * 2
	threeMegabytes = 1024 * 1024 * 3

	oneKilobyte = bytes.Repeat([]byte("a"), 1024)
)

func generateData(length int) []byte {
	return bytes.Repeat([]byte("a"), length)
}

func TestList(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_list/test.txt"),
		Body:   bytes.NewReader(oneKilobyte),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	files, err := s3fs.ReadDir(".")
	assert.NoError(err)

	assert.Len(files, 1)
	assert.Equal("test_list", files[0].Name())
	assert.Equal(true, files[0].IsDir())
}

func TestOpen(t *testing.T) {
	assert := require.New(t)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	_, err := s3fs.Stat("test_open.txt")
	assert.Error(err)

	_, err = s3fs.Open("test_open.txt")
	assert.Error(err)
}

func TestStat(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_stat.txt"),
		Body:   bytes.NewReader(oneKilobyte),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	finfo, err := s3fs.Stat("test_stat.txt")
	assert.NoError(err)

	assert.Equal("test_stat.txt", finfo.Name())
	assert.Equal(int64(1024), finfo.Size())
	assert.False(finfo.IsDir())
	assert.True(finfo.Mode().IsRegular())
	assert.Equal(fs.FileMode(0x0), finfo.Mode().Perm())
	assert.WithinDuration(time.Now(), finfo.ModTime(), 5*time.Second)
}

func TestSeek(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_seek.txt"),
		Body:   bytes.NewReader(oneKilobyte),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	t.Run("seek to start", func(t *testing.T) {
		f, err := s3fs.Open("test_seek.txt")
		assert.NoError(err)

		rdr, ok := f.(io.ReadSeekCloser)
		assert.True(ok)

		defer rdr.Close()

		n, err := rdr.Seek(512, io.SeekStart)
		assert.NoError(err)
		assert.Equal(int64(512), n)

		buf, err := io.ReadAll(rdr)
		assert.NoError(err)
		assert.Len(buf, 512)
	})

	t.Run("seek to end", func(t *testing.T) {
		f, err := s3fs.Open("test_seek.txt")
		assert.NoError(err)
		defer f.Close()
		rdr, ok := f.(io.ReadSeekCloser)
		assert.True(ok)
		defer rdr.Close()
		n, err := rdr.Seek(-512, io.SeekEnd)
		assert.NoError(err)
		assert.Equal(int64(512), n)
	})

	t.Run("seek to current", func(t *testing.T) {
		f, err := s3fs.Open("test_seek.txt")
		assert.NoError(err)
		defer f.Close()
		rdr, ok := f.(io.ReadSeekCloser)
		assert.True(ok)
		defer rdr.Close()
		n, err := rdr.Seek(512, io.SeekCurrent)
		assert.NoError(err)
		assert.Equal(int64(512), n)

		n, err = rdr.Seek(512, io.SeekCurrent)
		assert.NoError(err)
		assert.Equal(int64(1024), n)
	})
}

func TestReaderAt(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_reader_at.txt"),
		Body:   bytes.NewReader(oneKilobyte),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	f, err := s3fs.Open("test_reader_at.txt")
	assert.NoError(err)

	defer f.Close()

	rdr, ok := f.(io.ReaderAt)
	assert.True(ok)

	n, err := rdr.ReadAt(make([]byte, 512), 512)
	assert.NoError(err)
	assert.Equal(512, n)

	// zero byte read
	n, err = rdr.ReadAt(make([]byte, 0), 512)
	assert.NoError(err)
	assert.Equal(0, n)
}

func TestReaderAtBig(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_reader_at_big.txt"),
		Body:   bytes.NewReader(generateData(threeMegabytes)),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	f, err := s3fs.Open("test_reader_at_big.txt")
	assert.NoError(err)

	defer f.Close()

	rdr, ok := f.(io.ReaderAt)
	assert.True(ok)

	n, err := rdr.ReadAt(make([]byte, oneMegabyte), 0)
	assert.NoError(err)
	assert.Equal(oneMegabyte, n)

	n, err = rdr.ReadAt(make([]byte, twoMegabytes), 0)
	assert.NoError(err)
	assert.Equal(twoMegabytes, n)
}

func TestReadFile(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_read_big.txt"),
		Body:   bytes.NewReader(generateData(threeMegabytes)),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	data, err := fs.ReadFile(s3fs, "test_read_big.txt")
	assert.NoError(err)
	assert.Len(data, threeMegabytes)
}

func TestReadBigEOF(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_read_big_eof.txt"),
		Body:   bytes.NewReader(generateData(oneMegabyte)),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	f, err := s3fs.Open("test_read_big_eof.txt")
	assert.NoError(err)

	defer f.Close()

	n, err := io.ReadFull(f, make([]byte, twoMegabytes))
	assert.ErrorIs(err, io.ErrUnexpectedEOF)
	assert.Equal(oneMegabyte, n)
}

func TestRemove(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_remove.txt"),
		Body:   bytes.NewReader(generateData(oneMegabyte)),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	err = s3fs.Remove("test_remove.txt")
	assert.NoError(err)
}

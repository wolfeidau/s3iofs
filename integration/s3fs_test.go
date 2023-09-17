package integration

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/s3iofs"
)

var (
	oneKilobyte = bytes.Repeat([]byte("a"), 1024)
)

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
}

func TestSeek(t *testing.T) {
	assert := require.New(t)

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucketName),
		Key:    aws.String("test_stat.txt"),
		Body:   bytes.NewReader(oneKilobyte),
	})
	assert.NoError(err)

	s3fs := s3iofs.NewWithClient(testBucketName, client)

	f, err := s3fs.Open("test_stat.txt")
	assert.NoError(err)

	rdr, ok := f.(io.ReadSeekCloser)
	assert.True(ok)

	defer rdr.Close()

	n, err := rdr.Seek(512, 0)
	assert.NoError(err)
	assert.Equal(int64(512), n)

	buf, err := io.ReadAll(rdr)
	assert.NoError(err)
	assert.Len(buf, 512)
}

package s3iofs

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGetObjectAPI struct {
	getObject     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	listObjectsV2 func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	headObject    func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

func (m mockGetObjectAPI) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getObject(ctx, params, optFns...)
}

func (m mockGetObjectAPI) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.listObjectsV2(ctx, params, optFns...)
}
func (m mockGetObjectAPI) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m.headObject(ctx, params, optFns...)
}

func TestReadAt(t *testing.T) {

	cases := []struct {
		client func(t *testing.T) mockGetObjectAPI
		bucket string
		key    string
		expect []byte
	}{
		{
			client: func(t *testing.T) mockGetObjectAPI {
				return mockGetObjectAPI{
					getObject: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						t.Helper()

						assert.Equal(t, aws.String("fooBucket"), params.Bucket)
						assert.Equal(t, aws.String("barKey"), params.Key)
						assert.Equal(t, aws.String("bytes=0-1023"), params.Range)

						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(make([]byte, 1024))),
						}, nil
					},
					headObject: func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
						t.Helper()

						assert.Equal(t, aws.String("fooBucket"), params.Bucket)
						assert.Equal(t, aws.String("barKey"), params.Key)

						return &s3.HeadObjectOutput{
							ContentLength: 1024,
						}, nil
					},
				}
			},
			bucket: "fooBucket",
			key:    "barKey",
			expect: []byte("this is the body foo bar baz"),
		},
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert := require.New(t)

			// ctx := context.TODO()

			sysfs := NewWithClient(tt.bucket, tt.client(t))

			f, err := sysfs.Open(tt.key)
			assert.NoError(err)

			data := make([]byte, 1024)

			if rc, ok := f.(io.ReaderAt); ok {

				n, err := rc.ReadAt(data, 0)
				assert.NoError(err)
				assert.Equal(1024, n)
			}
		})
	}
}

func TestReadAtInt(t *testing.T) {
	if os.Getenv("TEST_BUCKET_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	// Load the Shared AWS Configuration (~/.aws/config)
	awscfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(err)

	client := s3.NewFromConfig(awscfg)

	res, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("TEST_BUCKET_NAME")),
		Key:    aws.String("1kfile"),
		Range:  aws.String("bytes=0-1023"),
	})
	assert.NoError(err)
	assert.Equal(int64(1024), res.ContentLength)

	data := make([]byte, 1024)

	n, err := res.Body.Read(data)
	assert.NoError(err) // assertion fails because we get back io.EOF
	assert.Equal(1024, n)
}

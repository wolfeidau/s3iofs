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

	type args struct {
		bucket string
		key    string
	}

	cases := []struct {
		client         func(t *testing.T) mockGetObjectAPI
		args           args
		expectData     []byte
		expectedLength int
	}{
		{
			client: func(t *testing.T) mockGetObjectAPI {
				return mockGetObjectAPI{
					getObject: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						t.Helper()

						require.Equal(t, aws.String("fooBucket"), params.Bucket)
						require.Equal(t, aws.String("barKey"), params.Key)
						require.Equal(t, aws.String("bytes=0-1023"), params.Range)

						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(make([]byte, 1024))),
						}, nil
					},
					headObject: func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
						t.Helper()

						require.Equal(t, aws.String("fooBucket"), params.Bucket)
						require.Equal(t, aws.String("barKey"), params.Key)

						return &s3.HeadObjectOutput{
							ContentLength: 1024,
						}, nil
					},
				}
			},
			args: args{
				bucket: "fooBucket",
				key:    "barKey",
			},
			expectData:     []byte("this is the body foo bar baz"),
			expectedLength: 1024,
		},
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert := require.New(t)

			sysfs := NewWithClient(tt.args.bucket, tt.client(t))

			f, err := sysfs.Open(tt.args.key)
			assert.NoError(err)

			data := make([]byte, tt.expectedLength)

			if rc, ok := f.(io.ReaderAt); ok {

				n, err := rc.ReadAt(data, 0)
				assert.NoError(err)
				assert.Equal(tt.expectedLength, n)
			}
		})
	}
}

func Test_buildRange(t *testing.T) {
	type args struct {
		offset int64
		length int64
	}
	tests := []struct {
		name string
		args args
		want *string
	}{
		{
			name: "should return 0-1023 for offset 0 and length 1024",
			args: args{
				offset: 0,
				length: 1024,
			},
			want: aws.String("bytes=0-1023"),
		},
		{
			// read the remainder of the file
			name: "should return nil for offset 1024 and length -1",
			args: args{
				offset: 1024,
				length: -1,
			},
			want: aws.String("bytes=1024-"),
		},
		{
			// a nil range will just read the entire file
			name: "should return nil for offset 0 and length -1",
			args: args{
				offset: 0,
				length: -1,
			},
			want: nil,
		},
		{
			// zero read will just read one byte as AWS doesn't support a zero byte range
			name: "should return nil for offset 0 and length -1",
			args: args{
				offset: 1024,
				length: 0,
			},
			want: aws.String("bytes=1024-1025"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRange(tt.args.offset, tt.args.length)
			require.Equal(t, aws.ToString(tt.want), aws.ToString(got))
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

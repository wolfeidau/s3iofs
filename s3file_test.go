package s3iofs

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const twoMegabytes = 1024 * 1024 * 2

type mockS3Client struct {
	mock.Mock
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.GetObjectOutput), args.Error(1)
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.HeadObjectOutput), args.Error(1)
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.ListObjectsV2Output), args.Error(1)
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.DeleteObjectOutput), args.Error(1)
}

func TestReadFile(t *testing.T) {
	type args struct {
		bucket string
		key    string
	}
	cases := []struct {
		client         func(t *testing.T) S3API
		args           args
		expectData     []byte
		expectedLength int
	}{
		{
			client: func(t *testing.T) S3API {
				t.Helper()
				mockClient := new(mockS3Client)

				mockClient.On("GetObject", mock.Anything, &s3.GetObjectInput{
					Bucket: aws.String("fooBucket"),
					Key:    aws.String("barKey"),
				}, mock.Anything).Return(&s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), twoMegabytes))),
					ContentLength: aws.Int64(twoMegabytes),
				}, nil).Once()

				return mockClient
			},
			args: args{
				bucket: "fooBucket",
				key:    "barKey",
			},
			expectData:     bytes.Repeat([]byte("a"), twoMegabytes),
			expectedLength: twoMegabytes,
		},
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert := require.New(t)

			sysfs := NewWithClient(tt.args.bucket, tt.client(t))

			data, err := fs.ReadFile(sysfs, tt.args.key)
			assert.NoError(err)
			assert.Equal(tt.expectData, data)
		})
	}
}

func TestReadAt(t *testing.T) {
	type args struct {
		bucket string
		key    string
		offset int64
	}

	cases := []struct {
		name           string
		client         func(t *testing.T) S3API
		args           args
		expectData     []byte
		expectedLength int
	}{
		{
			name: "ReadAt 1024 bytes from a 1024 byte file",
			client: func(t *testing.T) S3API {
				t.Helper()

				mockClient := new(mockS3Client)

				mockClient.On("GetObject", mock.Anything, &s3.GetObjectInput{
					Bucket: aws.String("fooBucket"),
					Key:    aws.String("barKey"),
				}, mock.Anything).Return(&s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), 1024))),
					ContentLength: aws.Int64(1024),
				}, nil).Once()

				mockClient.On("GetObject", mock.Anything, &s3.GetObjectInput{
					Bucket: aws.String("fooBucket"),
					Key:    aws.String("barKey"),
					Range:  aws.String("bytes=0-1023"),
				}, mock.Anything).Return(&s3.GetObjectOutput{
					Body: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), 1024))),
				}, nil).Once()

				return mockClient
			},
			args: args{
				bucket: "fooBucket",
				key:    "barKey",
				offset: 0,
			},
			expectData:     bytes.Repeat([]byte("a"), 1024),
			expectedLength: 1024,
		},
		{
			name: "ReadAt 1024 bytes from a 2048 byte file",
			client: func(t *testing.T) S3API {
				t.Helper()

				mockClient := new(mockS3Client)

				mockClient.On("GetObject", mock.Anything, &s3.GetObjectInput{
					Bucket: aws.String("fooBucket"),
					Key:    aws.String("barKey"),
				}, mock.Anything).Return(&s3.GetObjectOutput{
					ContentLength: aws.Int64(2048),
					Body:          io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), 1024))),
				}, nil).Once()

				mockClient.On("GetObject", mock.Anything, &s3.GetObjectInput{
					Bucket: aws.String("fooBucket"),
					Key:    aws.String("barKey"),
					Range:  aws.String("bytes=1024-2047"),
				}, mock.Anything).Return(&s3.GetObjectOutput{
					Body: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), 1024))),
				}, nil).Once()

				return mockClient
			},
			args: args{
				bucket: "fooBucket",
				key:    "barKey",
				offset: 1024,
			},
			expectData:     bytes.Repeat([]byte("a"), 1024),
			expectedLength: 1024,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			sysfs := NewWithClient(tt.args.bucket, tt.client(t))

			f, err := sysfs.Open(tt.args.key)
			assert.NoError(err)

			data := make([]byte, tt.expectedLength)

			if rc, ok := f.(io.ReaderAt); ok {

				n, err := rc.ReadAt(data, tt.args.offset)
				assert.NoError(err)
				assert.Equal(tt.expectedLength, n)
				assert.Equal(tt.expectData, data)
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

package s3iofs

import (
	"io/fs"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestS3FS_Stat(t *testing.T) {
	type fields struct {
		bucket   string
		s3client S3API
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    fs.FileInfo
		wantErr bool
	}{
		{
			name:   "should return directory for .",
			fields: fields{bucket: "test"},
			args:   args{name: "."},
			want: &s3File{
				name:   ".",
				bucket: "test",
				mode:   fs.ModeDir,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			s3fs := &S3FS{
				bucket:   tt.fields.bucket,
				s3client: tt.fields.s3client,
			}

			got, err := s3fs.Stat(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3FS.Stat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(got, tt.want)
		})
	}
}

func TestS3FS_ReadDirTable(t *testing.T) {
	type args struct {
		bucket string
	}

	modTime, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	require.NoError(t, err)

	cases := []struct {
		client func(t *testing.T) S3API
		args   args
		expect []fs.DirEntry
	}{
		{
			client: func(t *testing.T) S3API {
				t.Helper()

				mockClient := new(mockS3Client)

				mockClient.On("ListObjectsV2", mock.Anything, &s3.ListObjectsV2Input{
					Bucket:    aws.String("fooBucket"),
					Prefix:    aws.String(""),
					Delimiter: aws.String("/"),
				}, mock.Anything).Return(&s3.ListObjectsV2Output{
					Contents: []types.Object{
						{
							Key:          aws.String("file1"),
							LastModified: aws.Time(modTime),
						},
					},
				}, nil).Once()

				return mockClient
			},
			args: args{
				bucket: "fooBucket",
			},
			expect: []fs.DirEntry{&s3File{
				name:    "file1",
				bucket:  "",
				modTime: modTime,
			}},
		},
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert := require.New(t)

			sysfs := NewWithClient(tt.args.bucket, tt.client(t))
			got, err := sysfs.ReadDir(".")
			assert.NoError(err)
			assert.Len(got, 1)
		})
	}
}

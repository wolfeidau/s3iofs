package s3iofs

import (
	"io/fs"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/s3iofs/mocks"
)

func TestS3FS_Stat(t *testing.T) {
	assert := require.New(t)

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

func TestS3FS_ReadDir(t *testing.T) {
	assert := require.New(t)

	ctrl := gomock.NewController(t)

	s3client := mocks.NewMockS3API(ctrl)

	modTime, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	assert.NoError(err)

	s3client.EXPECT().ListObjectsV2(gomock.Any(), &s3.ListObjectsV2Input{
		Bucket:    aws.String("test"),
		Prefix:    aws.String(""),
		Delimiter: aws.String("/"),
	}).Return(&s3.ListObjectsV2Output{
		Contents: []types.Object{
			{
				Key:          aws.String("file1"),
				LastModified: aws.Time(modTime),
			},
		},
	}, nil)

	s3fs := &S3FS{
		bucket:   "test",
		s3client: s3client,
	}

	got, err := s3fs.ReadDir(".")
	assert.NoError(err)

	val := &s3File{
		name:    "file1",
		bucket:  "",
		modTime: modTime,
	}

	assert.Equal([]fs.DirEntry([]fs.DirEntry{(*s3File)(val)}), got)
}

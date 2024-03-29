package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/rs/zerolog/log"
	"github.com/wolfeidau/s3iofs"
)

func main() {
	// Load the Shared AWS Configuration (~/.aws/config) and enable request logging
	awscfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithClientLogMode(aws.LogRetries|aws.LogRequest),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load aws config")
	}

	client := s3.NewFromConfig(awscfg)

	s3fs := s3iofs.NewWithClient(os.Getenv("TEST_BUCKET_NAME"), client)

	err = fs.WalkDir(s3fs, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			fmt.Println("dir:", path)
			return nil
		}
		fmt.Println("file:", path)

		return nil
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to walk s3 bucket")
	}
}

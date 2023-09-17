package integration

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/logging"
	"github.com/ory/dockertest/v3"
)

var (
	endpoint       string
	client         *s3.Client
	testBucketName = "testbucket"
)

type Resolver struct {
	URL *url.URL
}

func (r *Resolver) ResolveEndpoint(_ context.Context, params s3.EndpointParameters) (transport.Endpoint, error) {
	u := *r.URL
	if params.Bucket != nil {
		u.Path += "/" + *params.Bucket
	}
	return transport.Endpoint{URI: u}, nil
}

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("failed to connect to docker: %s", err)
	}

	// uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Cmd:        []string{"server", "/data"},
		Tag:        "latest",
	})
	if err != nil {
		log.Fatalf("failed could not start resource: %s", err)
	}

	log.Println("id", resource.Container.ID)
	log.Println("created", resource.Container.Created)

	endpoint = fmt.Sprintf("http://%s", resource.GetHostPort("9000/tcp"))

	if err := pool.Retry(func() error {

		endpointURL, err := url.Parse(endpoint)
		if err != nil {
			log.Fatalf("failed to parse endpoint URL: %s", err)
		}

		var logmode aws.ClientLogMode

		if os.Getenv("AWS_DEBUG") != "" {
			logmode = aws.LogRetries | aws.LogRequest
		}

		client = s3.New(s3.Options{
			ClientLogMode:      logmode,
			Logger:             logging.NewStandardLogger(os.Stdout),
			EndpointResolverV2: &Resolver{URL: endpointURL},
			Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     "minioadmin",
					SecretAccessKey: "minioadmin",
				}, nil
			}),
		})

		// verify we can list buckets
		_, err = client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
		if err != nil {
			log.Printf("failed to list buckets: %s", err)
			return err
		}

		log.Println("client is connected")

		_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(testBucketName),
		})
		if err != nil {
			log.Printf("failed to create test bucket: %s", err)
			return err
		}

		log.Println("bucket created:", testBucketName)

		return nil
	}); err != nil {
		log.Fatalf("failed to connect to docker: %s", err)
	}

	code := m.Run()

	log.Println("code", code)

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("failed to purge resource: %s", err)
	}

	os.Exit(code)
}

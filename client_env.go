package dynago

import (
	"errors"
	"os"
)

/*
NewAwsClientFromEnv is a shortcut to create a new dynamo client set up for AWS executor.
It retrieves credentials from the environment variables of the running process.

Environment variables used:

* Access Key ID:     AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY
* Secret Access Key: AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY
* Region:            AWS_REGION
* Security Token:    AWS_SESSION_TOKEN (optional)
*/
func NewAwsClientFromEnv() (*Client, error) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY")
		if accessKey == "" {
			return nil, errors.New("AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY not found in environment")
		}
	}

	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_KEY")
		if secretKey == "" {
			return nil, errors.New("AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY not found in environment")
		}
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		return nil, errors.New("AWS_REGION not found in environment")
	}

	sessionToken := os.Getenv("AWS_SESSION_TOKEN")
	endpoint := "https://dynamodb." + region + ".amazonaws.com/"
	return NewClient(NewAwsExecutor(endpoint, region, accessKey, secretKey, sessionToken)), nil
}

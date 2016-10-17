package checkup

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

// Send notifications via Amazon SNS
type SNS struct {
	TopicArn        string `json:"topic_arn"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

func (s SNS) Notify(r []Result) error {
	config := &aws.Config{
		Region: &s.Region,
	}

	if s.AccessKeyID != "" && s.SecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.AccessKeyID, s.SecretAccessKey, "")
	}

	svc := newSns(session.New(), config)

	for _, res := range r {
		if !res.Healthy {
			if err := s.sendNotification(svc, res); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s SNS) sendNotification(svc snsSvc, r Result) error {
	params := &sns.PublishInput{
		TopicArn: aws.String(s.TopicArn),
		Subject:  aws.String(fmt.Sprintf("[Checkup] %s %s", r.Title, r.Status())),
		Message:  aws.String(r.String()),
	}
	_, err := svc.Publish(params)

	return err
}

var newSns = func(p client.ConfigProvider, cfgs ...*aws.Config) snsSvc {
	return sns.New(p, cfgs...)
}

type snsSvc interface {
	Publish(*sns.PublishInput) (*sns.PublishOutput, error)
}

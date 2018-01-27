package ssm

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/kelseyhightower/confd/log"
)

type Client struct {
	client        *ssm.SSM
	clientKinesis *kinesis.Kinesis
}

func New() (*Client, error) {
	// Create a session to share configuration, and load external configuration.
	sess := session.Must(session.NewSession())

	// Fail early, if no credentials can be found
	_, err := sess.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	var c *aws.Config
	if os.Getenv("SSM_LOCAL") != "" {
		log.Debug("SSM_LOCAL is set")
		endpoint := "http://localhost:8001"
		c = &aws.Config{
			Endpoint: &endpoint,
		}
	} else {
		c = nil
	}

	// Create the service's client with the session.
	svc := ssm.New(sess, c)
	k := kinesis.New(sess)
	return &Client{
		client:        svc,
		clientKinesis: k,
	}, nil
}

// GetValues retrieves the values for the given keys from AWS SSM Parameter Store
func (c *Client) GetValues(keys []string) (map[string]string, error) {
	vars := make(map[string]string)
	var err error
	for _, key := range keys {
		log.Debug("Processing key=%s", key)
		var resp map[string]string
		resp, err = c.getParametersWithPrefix(key)
		if err != nil {
			return vars, err
		}
		if len(resp) == 0 {
			resp, err = c.getParameter(key)
			if err != nil && err.(awserr.Error).Code() != ssm.ErrCodeParameterNotFound {
				return vars, err
			}
		}
		for k, v := range resp {
			vars[k] = v
		}
	}
	return vars, nil
}

func (c *Client) getParametersWithPrefix(prefix string) (map[string]string, error) {
	var err error
	parameters := make(map[string]string)
	params := &ssm.GetParametersByPathInput{
		Path:           aws.String(prefix),
		Recursive:      aws.Bool(true),
		WithDecryption: aws.Bool(true),
	}
	c.client.GetParametersByPathPages(params,
		func(page *ssm.GetParametersByPathOutput, lastPage bool) bool {
			for _, p := range page.Parameters {
				parameters[*p.Name] = *p.Value
			}
			return !lastPage
		})
	return parameters, err
}

func (c *Client) getParameter(name string) (map[string]string, error) {
	parameters := make(map[string]string)
	params := &ssm.GetParameterInput{
		Name:           aws.String(name),
		WithDecryption: aws.Bool(true),
	}
	resp, err := c.client.GetParameter(params)
	if err != nil {
		return parameters, err
	}
	parameters[*resp.Parameter.Name] = *resp.Parameter.Value
	return parameters, nil
}

type Event struct {
	Version    string    `json:"version"`
	ID         string    `json:"id"`
	DetailType string    `json:"detail-type"`
	Source     string    `json:"source"`
	Account    string    `json:"account"`
	Time       time.Time `json:"time"`
	Region     string    `json:"region"`
	Resources  []string  `json:"resources"`
	Detail     struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Operation string `json:"operation"`
	} `json:"detail"`
}

func (c *Client) WatchPrefix(prefix string, keys []string, waitIndex string, stopChan chan bool) (string, error) {
	log.Debug("Describing stream test")
	describeStreamOutput, err := c.clientKinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String("test"),
	})
	if err != nil {
		return waitIndex, err
	}
	log.Debug("Trying to get shard iterator for %s", *describeStreamOutput.StreamDescription.Shards[0].ShardId)
	var shardIterator *string
	if waitIndex == "" {
		getShardIteratorOutput, err := c.clientKinesis.GetShardIterator(&kinesis.GetShardIteratorInput{
			ShardId:           describeStreamOutput.StreamDescription.Shards[0].ShardId,
			StreamName:        aws.String("test"),
			ShardIteratorType: aws.String(kinesis.ShardIteratorTypeLatest),
		})
		if err != nil {
			return waitIndex, err
		}
		log.Debug("Got shard iterator %s", *getShardIteratorOutput.ShardIterator)
		shardIterator = getShardIteratorOutput.ShardIterator
	} else {
		log.Debug("Using previous shard iterator %s", waitIndex)
		shardIterator = &waitIndex
		time.Sleep(5 * time.Second)
	}
	for {
		getRecordsOutput, err := c.clientKinesis.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			return *shardIterator, err
		}
		log.Debug("Received records %#v", getRecordsOutput.Records)
		shardIterator = getRecordsOutput.NextShardIterator
		var event Event
		for _, record := range getRecordsOutput.Records {
			err = json.Unmarshal(record.Data, &event)
			if err != nil {
				return *shardIterator, err
			}
			log.Debug("Record data %#v original time %s", event, event.Time)
			for _, key := range keys {
				if strings.HasPrefix(event.Detail.Name, key) {
					return *shardIterator, nil
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}

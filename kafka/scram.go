package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// xdgSCRAMClient implements sarama.SCRAMClient using xdg-go/scram
type xdgSCRAMClient struct {
	hashGenerator scram.HashGeneratorFcn
	client        *scram.Client
	conv          *scram.ClientConversation
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	x.client, err = x.hashGenerator.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.conv = x.client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (string, error) {
	if x.conv == nil {
		return "", fmt.Errorf("SCRAM conversation not started")
	}
	response, err := x.conv.Step(challenge)
	return response, err
}

func (x *xdgSCRAMClient) Done() bool {
	if x.conv == nil {
		return true
	}
	return x.conv.Done()
}

var (
	scramClientGeneratorSHA256 = func() sarama.SCRAMClient {
		return &xdgSCRAMClient{hashGenerator: scram.SHA256}
	}
	scramClientGeneratorSHA512 = func() sarama.SCRAMClient {
		return &xdgSCRAMClient{hashGenerator: scram.SHA512}
	}
)

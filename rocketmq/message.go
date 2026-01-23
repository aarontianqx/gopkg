package rocketmq

import (
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

// Message is the unified message structure for both producing and consuming.
// It abstracts away the underlying RocketMQ SDK types.
type Message struct {
	// Topic is the destination topic name (required for sending)
	Topic string
	// Tag is the message tag, used for filtering and business-side routing
	Tag string
	// Keys are message index keys, used for message retrieval and tracing
	Keys []string
	// MessageGroup determines which queue the message is sent to.
	// Messages with the same MessageGroup are guaranteed to be sent to the same queue,
	// enabling ordered consumption when used with a FIFO topic.
	MessageGroup string
	// Body is the message payload
	Body []byte
	// Properties contains custom key-value pairs attached to the message
	Properties map[string]string
}

// toRocketMQMessage converts our Message to the underlying SDK message type for sending.
func (m *Message) toRocketMQMessage() *rmq_client.Message {
	msg := &rmq_client.Message{
		Topic: m.Topic,
		Body:  m.Body,
	}
	if m.Tag != "" {
		msg.SetTag(m.Tag)
	}
	if len(m.Keys) > 0 {
		msg.SetKeys(m.Keys...)
	}
	if m.MessageGroup != "" {
		msg.SetMessageGroup(m.MessageGroup)
	}
	if len(m.Properties) > 0 {
		for k, v := range m.Properties {
			msg.AddProperty(k, v)
		}
	}
	return msg
}

// toRocketMQDelayMessage converts our Message to an SDK message with delay timestamp.
func (m *Message) toRocketMQDelayMessage(delay time.Duration) *rmq_client.Message {
	msg := m.toRocketMQMessage()
	msg.SetDelayTimestamp(time.Now().Add(delay))
	return msg
}

// fromMessageView converts an SDK MessageView (received message) to our Message type.
func fromMessageView(mv *rmq_client.MessageView) *Message {
	msg := &Message{
		Topic:      mv.GetTopic(),
		Body:       mv.GetBody(),
		Keys:       mv.GetKeys(),
		Properties: mv.GetProperties(),
	}
	if tag := mv.GetTag(); tag != nil {
		msg.Tag = *tag
	}
	if group := mv.GetMessageGroup(); group != nil {
		msg.MessageGroup = *group
	}
	return msg
}

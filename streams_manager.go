package producer

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/suicidejack/goprimitives"
)

// KinesisProducer a high level kinesis producer. Handles sending to multiple
// streams and batching in an async manner.
type KinesisProducer interface {
	Close()
	Input() chan<- *KinesisMessage
	Successes() <-chan *KinesisMessage
	Errors() <-chan *KinesisError
}

// KinesisStreamManager manages multiple KinesisStreamProducers
// and implements the KinesisProduver interface.
type KinesisStreamManager struct {
	successes chan *KinesisMessage
	errors    chan *KinesisError
	input     chan *KinesisMessage
	killInput chan bool
	streams   map[string]*KinesisStreamProducer
	config    *KinesisProducerConfig
}

// KinesisError TODO
type KinesisError struct {
	ErrorCode    string
	ErrorMessage string
	AWSError     error
	Message      *KinesisMessage
}

// Log error level output
func (e KinesisError) Log(msg string) {
	log.WithFields(log.Fields{
		"ErrorCode":    e.ErrorCode,
		"ErrorMessage": e.ErrorMessage,
		"AWSError":     e.AWSError,
	}).Error("KinesisError: " + msg)
}

func (e KinesisError) Error() string {
	return fmt.Sprintf("KinesisError: Code[%s]: %s", e.ErrorCode, e.ErrorMessage)
}

// KinesisMessage TODO
type KinesisMessage struct {
	Data         []byte
	Stream       string
	PartitionKey string
	Metadata     interface{}
}

// Log debug level
func (k KinesisMessage) Log(msg string) {
	log.WithFields(log.Fields{
		"stream":       k.Stream,
		"partitionKey": k.PartitionKey,
	}).Debug(msg)
}

// KinesisProducerConfig for a kinesis producer (high level, stream, etc.)
type KinesisProducerConfig struct {
	// max time that should elapse before sending buffered requests to kinesis
	FlushFrequency *goprimitives.Duration `json:"flushFrequency"`
	// min num number of buffered bytes to have before sending a request to kinesis
	FlushBytes int `json:"flushBytes"`
	// min number of buffered requests before sending a request to kinesis
	FlushMessages int `json:"flushMessages"`
	// max number of buffered requests before sending a request to kinesis
	FlushMaxMessages int `json:"flushMaxMessages"`
	// max number of outstanding requests to a single kinesis stream
	MaxOpenRequests int `json:"maxOpenRequests"`
	// max size of the data blob (kinesis record) that can be sent to kinesis (no more then 50KB)
	MaxMessageBytes int `json:"maxMessageBytes"`
	// run the AWS client in debug mode
	AWSDebugMode bool `json:"awsDebugMode"`
	// max number of attempts when sending a request to kinesis
	MaxRetries int `json:"maxRetries"`
	// the size of internal buffers
	BufferSize int `json:"bufferSize"`
	// should the producer return successfully acknowledged messages from kinesis (you must read
	// from the Successes() channel if this is true
	AckSuccess bool `json:"AckSuccesses"`
}

func (c KinesisProducerConfig) String() string {
	return fmt.Sprintf("FlushFrequency: %s, "+
		"FlushBytes: %d, "+
		"FlushMessages: %d, "+
		"FlushMaxMessages: %d, "+
		"MaxOpenRequests: %d, "+
		"MaxMessageBytes: %d, "+
		"AWSDebugMode: %t, "+
		"MaxRetries: %d, "+
		"BufferSize: %d, "+
		"AckSuccess: %t",
		c.FlushFrequency,
		c.FlushBytes,
		c.FlushMessages,
		c.FlushMaxMessages,
		c.MaxOpenRequests,
		c.MaxMessageBytes,
		c.AWSDebugMode,
		c.MaxRetries,
		c.BufferSize,
		c.AckSuccess,
	)
}

// NewKinesisStreamManager creates a new kinesis manager. It handles:
// batching messages
func NewKinesisStreamManager(opts *KinesisProducerConfig) (manager *KinesisStreamManager) {
	manager = &KinesisStreamManager{
		successes: make(chan *KinesisMessage, opts.BufferSize),
		errors:    make(chan *KinesisError, opts.BufferSize),
		input:     make(chan *KinesisMessage, opts.BufferSize),
		streams:   make(map[string]*KinesisStreamProducer),
		killInput: make(chan bool, 1),
		config:    opts,
	}
	go manager.getMessages()
	return
}

func (m *KinesisStreamManager) getMessages() {
	for msg := range m.input {
		producer, exists := m.streams[msg.Stream]
		if !exists {
			m.streams[msg.Stream] = newManagedStreamProducer(m.config, m.successes, m.errors)
			producer, _ = m.streams[msg.Stream]
		}
		//msg.Log("KinesisStreamManager: got a message")
		producer.Input() <- msg
	}
	m.killInput <- true
}

// Close TODO
func (m *KinesisStreamManager) Close() {
	log.Warn("closing kinesis producer")
	close(m.input)
	<-m.killInput
	for stream, producer := range m.streams {
		log.WithField("stream", stream).Warn("closing stream producer")
		producer.Close()
	}
	close(m.successes)
	close(m.errors)
}

// Input TODO
func (m *KinesisStreamManager) Input() chan<- *KinesisMessage {
	return m.input
}

// Successes TODO
func (m *KinesisStreamManager) Successes() <-chan *KinesisMessage {
	return m.successes
}

// Errors TODO
func (m *KinesisStreamManager) Errors() <-chan *KinesisError {
	return m.errors
}

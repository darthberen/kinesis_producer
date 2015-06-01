package producer

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"github.com/suicidejack/throttled"
)

// KinesisStreamProducerError internal error within the producer
type KinesisStreamProducerError string

// Error TODO
func (e KinesisStreamProducerError) Error() string {
	//log.Debug("we here and we doing it with some str")
	return fmt.Sprintf("KinesisStreamProducerError: %s", string(e))
}

// KinesisStreamProducer producer for a single stream.  Handles batching in an async manner.
// Implements the KinesisProducer interface.
type KinesisStreamProducer struct {
	successes chan *KinesisMessage
	errors    chan *KinesisError
	input     chan *KinesisMessage
	killInput chan bool
	client    *kinesis.Kinesis
	config    *KinesisProducerConfig
	isManaged bool
}

func getAWSConfig(runInDebugMode bool) *aws.Config {
	logLevel := uint(0)
	if runInDebugMode {
		logLevel = 5
	}
	return aws.DefaultConfig.Merge(&aws.Config{
		LogLevel: logLevel,
	})
}

// NewStreamProducer TODO
func NewStreamProducer(opts *KinesisProducerConfig) (producer *KinesisStreamProducer) {
	producer = &KinesisStreamProducer{
		client:    kinesis.New(getAWSConfig(opts.AWSDebugMode)),
		successes: make(chan *KinesisMessage, opts.BufferSize),
		errors:    make(chan *KinesisError, opts.BufferSize),
		input:     make(chan *KinesisMessage, opts.BufferSize),
		killInput: make(chan bool, 1),
		config:    opts,
		isManaged: false,
	}
	go producer.getMessages()
	return
}

// NewStreamProducer TODO
func newManagedStreamProducer(opts *KinesisProducerConfig, successes chan *KinesisMessage, errors chan *KinesisError) (producer *KinesisStreamProducer) {
	producer = NewStreamProducer(opts)
	producer.successes = successes
	producer.errors = errors
	producer.isManaged = true
	return
}

// ValidateStream checks if the stream exists (or even if you have a valid AWS connection)
// and returns and error if the stream does not exist
// TODO: this should page through results
func (p *KinesisStreamProducer) ValidateStream(streamName string) error {
	input := &kinesis.ListStreamsInput{
		ExclusiveStartStreamName: nil,
		Limit: aws.Long(1000),
	}
	if out, err := p.client.ListStreams(input); err != nil {
		return err
	} else if len(out.StreamNames) >= 1 {
		for _, stream := range out.StreamNames {
			log.WithField("streamName", stringPtrToString(stream)).Debug("found a stream")
			if stringPtrToString(stream) == streamName {
				return nil
			}
		}
	}
	return KinesisStreamProducerError(fmt.Sprintf("stream [%s] not found", streamName))
}

func (p *KinesisStreamProducer) getMessages() {
	twg := throttled.NewWaitGroup(p.config.MaxOpenRequests)
	buffer := make([]*KinesisMessage, p.config.FlushMaxMessages, p.config.FlushMaxMessages)
	addToBufferSize := p.config.FlushMaxMessages - 1
	bufferByteSize := 0
	bufferedCount := 0
	flushTimer := time.NewTimer(p.config.FlushFrequency.TimeDuration())

Processing:
	for {
		select {
		case <-flushTimer.C:
			// this exits the select block
		case msg, hasData := <-p.input:
			switch {
			case !hasData: // input channel was closed - indicates producer was told to close
				break Processing
			case hasData && msg == nil:
				errMsg := "attempted to send a nil message"
				p.handleError(msg, KinesisStreamProducerError(errMsg), "", "KinesisStreamProducer: "+errMsg)
				continue Processing
			case len(msg.Data) > p.config.MaxMessageBytes:
				errMsg := "message size exceeds configuration limit"
				p.handleError(msg, KinesisStreamProducerError(errMsg), "", "KinesisStreamProducer: "+errMsg)
				continue Processing
			case p.config.FlushBytes > bufferByteSize+len(msg.Data) && bufferedCount < addToBufferSize:
				// buffering request because we want to flush more bytes
				fallthrough
			case p.config.FlushMessages > bufferedCount+1 && bufferedCount < addToBufferSize:
				// buffering request because we want to flush more total messages
				fallthrough
			case twg.PeekThrottled() && bufferedCount < addToBufferSize:
				// buffer the request and wait for a flush or another message
				buffer[bufferedCount] = msg
				bufferByteSize += len(msg.Data)
				bufferedCount++
				continue Processing
			default:
				// add request to buffer and flush
				buffer[bufferedCount] = msg
				bufferByteSize += len(msg.Data)
				bufferedCount++
			}
		}
		if bufferedCount == 1 {
			twg.Add()
			go p.flushSingleRecord(buffer[0], twg)
			bufferedCount = 0
			bufferByteSize = 0
		} else if bufferedCount > 1 {
			twg.Add()
			go p.flushRecords(buffer, bufferedCount, twg)
			buffer = make([]*KinesisMessage, p.config.FlushMaxMessages, p.config.FlushMaxMessages)
			bufferedCount = 0
			bufferByteSize = 0
		}
		flushTimer.Reset(p.config.FlushFrequency.TimeDuration())
	}
	twg.Wait()
	if bufferedCount > 0 {
		p.flushRecords(buffer, bufferedCount, twg)
	}
	log.Debug("KinesisStreamProducer: closing message loop")
	p.killInput <- true
	log.Debug("KinesisStreamProducer: closed message loop")
}

func (p *KinesisStreamProducer) handleError(msg *KinesisMessage, err error, errorCode, errorMessage string) {
	p.errors <- &KinesisError{
		Message:      msg,
		AWSError:     err,
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
	}
}

func (p *KinesisStreamProducer) flushRecords(msgs []*KinesisMessage, size int, twg *throttled.WaitGroup) {
	defer twg.Done()
	request := &kinesis.PutRecordsInput{
		StreamName: aws.String(msgs[0].Stream),
		Records:    make([]*kinesis.PutRecordsRequestEntry, size, size),
	}
	for i := 0; i < size; i++ {
		request.Records[i] = &kinesis.PutRecordsRequestEntry{
			Data:         msgs[i].Data,
			PartitionKey: aws.String(msgs[0].PartitionKey),
		}
	}
	out, err := p.client.PutRecords(request)
	log.WithField("size", size).Debug("KinesisStreamProducer: flushed records")
	if err != nil {
		for i := 0; i < size; i++ {
			p.handleError(msgs[i], err, "", "KinesisStreamProducer: PutRecords request failed")
		}
		return
	}
	for i, record := range out.Records {
		if record.ErrorCode != nil || record.ErrorMessage != nil {
			p.handleError(msgs[i], err, stringPtrToString(record.ErrorCode), stringPtrToString(record.ErrorMessage))
		} else if p.config.AckSuccess {
			p.successes <- msgs[i]
		}
	}
}

func (p *KinesisStreamProducer) flushSingleRecord(msg *KinesisMessage, twg *throttled.WaitGroup) {
	defer twg.Done()
	_, err := p.client.PutRecord(&kinesis.PutRecordInput{
		Data:         msg.Data,
		StreamName:   aws.String(msg.Stream),
		PartitionKey: aws.String(msg.PartitionKey),
	})
	log.Debug("KinesisStreamProducer: flushed single record")
	if err != nil {
		p.handleError(msg, err, "", "KinesisStreamProducer: PutRecord request failed")
	} else if p.config.AckSuccess {
		p.successes <- msg
	}
}

// Close this closes the producer. It must be closed before the producer passes out
// of scope as you will leave dangling goroutines, memory, etc.
func (p *KinesisStreamProducer) Close() {
	log.Warn("KinesisStreamProducer: closing stream producer")
	close(p.input)
	<-p.killInput
	if !p.isManaged {
		close(p.successes)
		close(p.errors)
	}
}

// Input TODO
func (p *KinesisStreamProducer) Input() chan<- *KinesisMessage {
	return p.input
}

// Successes TODO
func (p *KinesisStreamProducer) Successes() <-chan *KinesisMessage {
	return p.successes
}

// Errors TODO
func (p *KinesisStreamProducer) Errors() <-chan *KinesisError {
	return p.errors
}

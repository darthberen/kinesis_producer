package main

import (
	"flag"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/suicidejack/goprimitives"
	"github.com/suicidejack/kinesis_producer"
)

var (
	streamName string
	sendTotal  int

	bufferSize       int
	verbose          bool
	flushFreq        *goprimitives.Duration
	flushBytes       int
	flushMessages    int
	flushMaxMessages int
	maxOpenRequests  int
	maxMessageBytes  int
	maxRetries       int
)

func init() {
	flag.StringVar(&streamName, "stream-name", "your_stream", "the kinesis stream to read from")
	flag.IntVar(&sendTotal, "send-total", 100, "total number of records to send to kinesis")
	flag.IntVar(&bufferSize, "buffer-size", 100000, "size of the internal buffer that holds records to process")
	flag.BoolVar(&verbose, "v", false, "verbose mode")
	flag.Var(flushFreq, "flush-freq", "how frequently should the producer flush messages")
	flag.IntVar(&flushBytes, "flush-bytes", 1000000, "maximum amount of bytes the producer should buffer before flushing to kinesis")
	flag.IntVar(&flushMessages, "flush-messages", 100, "minimum amount of messages to buffer before flushing to kinesis")
	flag.IntVar(&flushMaxMessages, "flush-max-messages", 500, "maximum amount of messages to buffer before flushing to kinesis")
	flag.IntVar(&maxOpenRequests, "max-open-requests", 10, "maximum amount of requests that be open to a single kinesis stream")
	flag.IntVar(&maxMessageBytes, "max-message-bytes", 50000, "maximum amount of bytes for a single kinesis record")
	flag.IntVar(&maxRetries, "max-retries", 1, "maximum amount of times to attemp resending a request to kinesis")
}

func main() {
	flag.Parse()

	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	if flushFreq == nil || !flushFreq.IsPositive() {
		flushFreq, _ = goprimitives.NewDuration("1s")
	}

	cfg := &producer.KinesisProducerConfig{
		FlushFrequency:   flushFreq,
		FlushBytes:       flushBytes,
		FlushMessages:    flushMessages,
		FlushMaxMessages: flushMaxMessages,
		MaxOpenRequests:  maxOpenRequests,
		MaxMessageBytes:  maxMessageBytes,
		AWSDebugMode:     false,
		MaxRetries:       maxRetries,
		BufferSize:       bufferSize,
		AckSuccess:       true,
	}

	streamProducer := producer.NewStreamProducer(cfg)
	if err := streamProducer.ValidateStream(streamName); err != nil {
		log.WithField("error", err).Error("unable to validate stream")
		return
	}

	log.WithFields(log.Fields{
		"stream":    streamName,
		"sendTotal": sendTotal,
	}).Info("sending messages to kinesis")

	time.Sleep(3000 * time.Millisecond)
	for i := 0; i < sendTotal; i++ {
		streamProducer.Input() <- &producer.KinesisMessage{
			Data:         []byte(fmt.Sprintf("data record %d", i)),
			Stream:       streamName,
			PartitionKey: fmt.Sprintf("%d", i),
		}
	}

	for {
		select {
		case <-streamProducer.Successes():
			log.Debug("got a success message")
		case kinesisError := <-streamProducer.Errors():
			kinesisError.Log("received an error")
		}
	}

	time.Sleep(10000 * time.Millisecond)
	//streamProducer.Close()
}

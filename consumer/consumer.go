// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"github.com/Shopify/sarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"log"
	"os"
	"strings"
)

var (
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
)

// initTracer creates a new trace provider instance and registers it as global trace provider.
func initTracer() func() {
	// Create and install Jaeger export pipeline.
	flush, err := jaeger.InstallNewPipeline(
		jaeger.WithCollectorEndpoint("http://<jaeger-endpoint:port>/api/traces"),
		jaeger.WithProcess(jaeger.Process{
			ServiceName: "kafka-consumer",
			Tags: []label.KeyValue{
				label.String("exporter", "jaeger"),
			},
		}),
		jaeger.WithSDK(&sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
	)
	if err != nil {
		log.Fatal(err)
	}
	return flush
}

func main() {
	flush := initTracer()
	defer flush()

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	startConsumerGroup(brokerList)

	select {}
}

func startConsumerGroup(brokerList []string) {
	consumerGroupHandler := Consumer{}
	// Wrap instrumentation
	propagators := propagation.TraceContext{}
	handler := otelsarama.WrapConsumerGroupHandler(&consumerGroupHandler,otelsarama.WithPropagators(propagators))

	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokerList, "example", config)
	if err != nil {
		log.Fatalln("Failed to start sarama consumer group:", err)
	}

	topicName, exists := os.LookupEnv("KAFKA_TOPIC")
	if !exists {
		log.Println("Using default topic name test-topic")
		topicName = "test-topic"
	}

	err = consumerGroup.Consume(context.Background(), []string{topicName}, handler)
	if err != nil {
		log.Fatalln("Failed to consume via handler:", err)
	}
}

func printMessage(msg *sarama.ConsumerMessage) {
	// Extract tracing info from message

	propagators := propagation.TraceContext{}
	ctx := propagators.Extract(context.Background(), otelsarama.NewConsumerMessageCarrier(msg))

	tr := otel.Tracer("consumer")


	// Create a span.

	_, span := tr.Start(ctx, "consume message")


	defer span.End()

	// Inject current span context, so any further processing can use it to propagate span.
	propagators.Inject(ctx, otelsarama.NewConsumerMessageCarrier(msg))

	// Emulate Work Loads (or any further processing as needed)
	//time.Sleep(1 * time.Second)

	span.SetAttributes(label.String("test-consumer-span-key","test-consumer-span-value"))

	// Set any additional attributes that might make sense
	//span.SetAttributes(label.String("consumed message at offset",strconv.FormatInt(int64(msg.Offset),10)))
	//span.SetAttributes(label.String("consumed message to partition",strconv.FormatInt(int64(msg.Partition),10)))
	//span.SetAttributes(label.String("message_bus.destination",msg.Topic))


	log.Println("Successful to read message: ", string(msg.Value), "at offset of ", msg.Offset)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		printMessage(message)
		session.MarkMessage(message, "")
	}

	return nil
}

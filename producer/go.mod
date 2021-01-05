module go-tracing-kafka/producer

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama v0.15.1
	go.opentelemetry.io/otel v0.15.0
	go.opentelemetry.io/otel/exporters/trace/jaeger v0.15.0
	go.opentelemetry.io/otel/sdk v0.15.0
)

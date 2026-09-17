package mkafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// consumerOnlyProps lists librdkafka properties that only a consumer instance
// understands. A producer accepts them but logs
//
//	CONFWARN|... property <k> is a consumer property and will be ignored
//
// and drops them, so passing them through is never useful — only noisy.
//
// NewKafkaConfig (aionj ikafka) builds ONE ExtConfig map shared by both the
// producer and the consumer, so consumer-only keys reach the producer unless
// filtered here. Measured against librdkafka 1.9.2 (confluent-kafka-go v1.9.2)
// by handing each key to NewProducer and recording which ones it warns about;
// keys shared with the producer (security.protocol, ssl.*, acks, linger.ms…)
// are deliberately absent so TLS wiring keeps working.
var consumerOnlyProps = map[string]struct{}{
	"group.id":                      {},
	"group.instance.id":             {},
	"auto.offset.reset":             {},
	"max.poll.interval.ms":          {},
	"enable.auto.commit":            {},
	"enable.auto.offset.store":      {},
	"session.timeout.ms":            {},
	"heartbeat.interval.ms":         {},
	"partition.assignment.strategy": {},
	"fetch.min.bytes":               {},
	"fetch.wait.max.ms":             {},
	"fetch.message.max.bytes":       {},
	"max.partition.fetch.bytes":     {},
	"isolation.level":               {},
	"check.crcs":                    {},
}

// applyExtConfig copies ExtConfig into configMap. When forProducer is true,
// consumer-only properties are skipped.
//
// The consumer keeps receiving every key: it sets group.id / auto.offset.reset
// itself before this runs, and ExtConfig is applied last so an explicit
// override still wins.
func applyExtConfig(configMap kafka.ConfigMap, ext map[string]interface{}, forProducer bool) {
	for k, v := range ext {
		if forProducer {
			if _, consumerOnly := consumerOnlyProps[k]; consumerOnly {
				continue
			}
		}
		configMap[k] = v
	}
}

package mkafka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Real ExtConfig as aionj ikafka builds it for the vb-aion cluster: one shared
// map holding a consumer-only key next to the TLS wiring.
func sharedExtConfig() map[string]interface{} {
	return map[string]interface{}{
		"max.poll.interval.ms":                300000,
		"security.protocol":                   "SSL",
		"ssl.ca.certificate.stores":           "Root",
		"enable.ssl.certificate.verification": true,
	}
}

func TestProducerDropsConsumerOnlyKeepsTLS(t *testing.T) {
	cm := kafka.ConfigMap{}
	applyExtConfig(cm, sharedExtConfig(), true)

	if _, ok := cm["max.poll.interval.ms"]; ok {
		t.Error("max.poll.interval.ms reached the producer; CONFWARN would still fire")
	}
	// TLS wiring must survive — dropping it would break the kroxylicious path.
	for _, k := range []string{"security.protocol", "ssl.ca.certificate.stores", "enable.ssl.certificate.verification"} {
		if _, ok := cm[k]; !ok {
			t.Errorf("%s was filtered out; SSL would break", k)
		}
	}
}

func TestConsumerKeepsEveryKey(t *testing.T) {
	ext := sharedExtConfig()
	cm := kafka.ConfigMap{}
	applyExtConfig(cm, ext, false)

	if len(cm) != len(ext) {
		t.Fatalf("consumer got %d keys, want all %d", len(cm), len(ext))
	}
	if cm["max.poll.interval.ms"] != 300000 {
		t.Error("consumer lost max.poll.interval.ms")
	}
}

// The bug this fixes: librdkafka must no longer report the key as ignored.
func TestRealProducerNoConsumerOnlyProps(t *testing.T) {
	cm := kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	applyExtConfig(cm, map[string]interface{}{"max.poll.interval.ms": 300000}, true)

	p, err := kafka.NewProducer(&cm)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	if v, err := cm.Get("max.poll.interval.ms", nil); err == nil && v != nil {
		t.Errorf("producer config still carries consumer-only key: %v", v)
	}
}

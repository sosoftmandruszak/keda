package scalers

import (
	"context"
	"net/http"
	"testing"
)

type parseStanMetadataTestData struct {
	metadata   map[string]string
	authParams map[string]string
	isError    bool
}

type stanMetricIdentifier struct {
	metadataTestData *parseStanMetadataTestData
	scalerIndex      int
	name             string
}

var testStanMetadata = []parseStanMetadataTestData{
	// nothing passed
	{map[string]string{}, map[string]string{}, true},
	// Missing subject name, should fail
	{map[string]string{"natsServerMonitoringEndpoint": "stan-nats-ss", "queueGroup": "grp1", "durableName": "ImDurable"}, map[string]string{}, true},
	// Missing durable name, should fail
	{map[string]string{"natsServerMonitoringEndpoint": "stan-nats-ss", "queueGroup": "grp1", "subject": "mySubject"}, map[string]string{}, true},
	// Missing nats server monitoring endpoint, should fail
	{map[string]string{"queueGroup": "grp1", "subject": "mySubject"}, map[string]string{}, true},
	// All good.
	{map[string]string{"natsServerMonitoringEndpoint": "stan-nats-ss", "queueGroup": "grp1", "durableName": "ImDurable", "subject": "mySubject"}, map[string]string{}, false},
	// natsServerMonitoringEndpoint is defined in authParams
	{map[string]string{"queueGroup": "grp1", "durableName": "ImDurable", "subject": "mySubject"}, map[string]string{"natsServerMonitoringEndpoint": "stan-nats-ss"}, false},
	// Missing nats server monitoring endpoint , should fail
	{map[string]string{"queueGroup": "grp1", "durableName": "ImDurable", "subject": "mySubject"}, map[string]string{"natsServerMonitoringEndpoint": ""}, true},
}

var stanMetricIdentifiers = []stanMetricIdentifier{
	{&testStanMetadata[4], 0, "s0-stan-grp1-ImDurable-mySubject"},
	{&testStanMetadata[4], 1, "s1-stan-grp1-ImDurable-mySubject"},
}

func TestStanParseMetadata(t *testing.T) {
	for _, testData := range testStanMetadata {
		_, err := parseStanMetadata(&ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: testData.authParams})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		} else if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestStanGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range stanMetricIdentifiers {
		ctx := context.Background()
		meta, err := parseStanMetadata(&ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata, ScalerIndex: testData.scalerIndex})
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		mockStanScaler := stanScaler{
			channelInfo: nil,
			metadata:    meta,
			httpClient:  http.DefaultClient,
		}

		metricSpec := mockStanScaler.GetMetricSpecForScaling(ctx)
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

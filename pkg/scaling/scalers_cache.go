/*
Copyright 2021 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaling

import (
	"context"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/eventreason"
	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scaling/scaledjob"
	"k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sync"
)

type ScalersCache struct {
	scalers []scalers.Scaler
	builders []func() (scalers.Scaler, error)
	lock *sync.RWMutex
	logger logr.Logger
	recorder record.EventRecorder
	object *kedav1alpha1.WithTriggers
}

func (c *ScalersCache) GetScalers() []scalers.Scaler {
	c.lock.RLock()
	defer c.lock.Unlock()
	return c.scalers
}

func (c *ScalersCache) GetPushScalers() []scalers.PushScaler {
	var result []scalers.PushScaler
	for _, s := range c.scalers {
		if ps, ok := s.(scalers.PushScaler); ok {
			result = append(result, ps)
		}
	}
	return result
}

func (c *ScalersCache) IsScaledObjectActive(ctx context.Context) (bool, bool, []external_metrics.ExternalMetricValue) {
	isActive := false
	isError := false
	for i, scaler := range c.scalers {
		isTriggerActive, err := scaler.IsActive(ctx)
		if err != nil {
			scaler, err = c.builders[i]()
			if err == nil {
				c.lock.Lock()
				c.scalers[i] = scaler
				c.lock.Unlock()
			}
		}

		if err != nil {
			c.logger.V(1).Info("Error getting scale decision", "Error", err)
			isError = true
			c.recorder.Event(c.object, corev1.EventTypeWarning, eventreason.KEDAScalerFailed, err.Error())
		} else if isTriggerActive {
			isActive = true
			if externalMetricsSpec := scaler.GetMetricSpecForScaling()[0].External; externalMetricsSpec != nil {
				c.logger.V(1).Info("Scaler for scaledObject is active", "Metrics Name", externalMetricsSpec.Metric.Name)
			}
			if resourceMetricsSpec := scaler.GetMetricSpecForScaling()[0].Resource; resourceMetricsSpec != nil {
				c.logger.V(1).Info("Scaler for scaledObject is active", "Metrics Name", resourceMetricsSpec.Name)
			}
			break
		}
	}

	return isActive, isError, []external_metrics.ExternalMetricValue{}
}

func (c *ScalersCache) IsScaledJobActive(ctx context.Context, scaledJob *kedav1alpha1.ScaledJob) (bool, int64, int64) {
	return scaledjob.GetScaleMetrics(ctx, c.scalers, scaledJob, c.recorder)
}

func (c *ScalersCache) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	var metrics []external_metrics.ExternalMetricValue
	for _, s := range c.scalers {
		m, err := s.GetMetrics(ctx, metricName, metricSelector)
		if err != nil {
			return metrics, err
		}
		metrics = append(metrics, m...)
	}

	return metrics, nil
}

func (c *ScalersCache) GetMetricSpecForScaling() []v2beta2.MetricSpec {
	var spec []v2beta2.MetricSpec
	for _, scaler := range c.scalers {
		spec = append(spec, scaler.GetMetricSpecForScaling()...)
	}
	return spec
}

func (c *ScalersCache) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	scalers := c.scalers
	c.scalers = nil
	for _, s := range scalers {
		err := s.Close()
		if err != nil {
			c.logger.Error(err, "error closing scaler", "scaler", s)
		}
	}
}
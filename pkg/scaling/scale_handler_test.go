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
	"errors"
	"github.com/golang/mock/gomock"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	mock_scalers "github.com/kedacore/keda/v2/pkg/mock/mock_scaler"
	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/stretchr/testify/assert"
	"k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sync"
	"testing"
)

func TestCheckScaledObjectScalersWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)

	scaler := mock_scalers.NewMockScaler(ctrl)
	scalers := []scalers.Scaler{scaler}
	scaledObject := &kedav1alpha1.WithTriggers{}
	cache := ScalersCache{
		scalers: scalers,
		object: scaledObject,
		lock: &sync.RWMutex{},
		logger: logf.Log.WithName("scalehandler"),
		recorder:          recorder,
	}

	scaler.EXPECT().IsActive(gomock.Any()).Return(false, errors.New("Some error"))
	scaler.EXPECT().Close()

	isActive, isError, _ := cache.IsScaledObjectActive(context.TODO())

	assert.Equal(t, false, isActive)
	assert.Equal(t, true, isError)
}

func TestCheckScaledObjectFindFirstActiveIgnoringOthers(t *testing.T) {
	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)
	activeScaler := mock_scalers.NewMockScaler(ctrl)
	failingScaler := mock_scalers.NewMockScaler(ctrl)
	scaledObject := &kedav1alpha1.ScaledObject{}

	metricsSpecs := []v2beta2.MetricSpec{createMetricSpec(1)}

	activeScaler.EXPECT().IsActive(gomock.Any()).Return(true, nil)
	activeScaler.EXPECT().GetMetricSpecForScaling().Times(2).Return(metricsSpecs)
	activeScaler.EXPECT().Close()
	failingScaler.EXPECT().Close()
	obj, err := asDuckWithTriggers(scaledObject)
	assert.Nil(t, err)
	scalersCache := ScalersCache{
		scalers: []scalers.Scaler{activeScaler, failingScaler},
		logger: logf.Log.WithName("scalercache"),
		object: obj,
		lock: &sync.RWMutex{},
		builders: []func() (scalers.Scaler, error){func() (scalers.Scaler, error) {
			return mock_scalers.NewMockScaler(ctrl), nil
		}, func() (scalers.Scaler, error) {
			return mock_scalers.NewMockScaler(ctrl), nil
		}},
		recorder: recorder,
	}

	isActive, isError, _ := scalersCache.IsScaledObjectActive(context.TODO())
	scalersCache.Close()

	assert.Equal(t, true, isActive)
	assert.Equal(t, false, isError)
}

func createMetricSpec(averageValue int) v2beta2.MetricSpec {
	qty := resource.NewQuantity(int64(averageValue), resource.DecimalSI)
	return v2beta2.MetricSpec{
		External: &v2beta2.ExternalMetricSource{
			Target: v2beta2.MetricTarget{
				AverageValue: qty,
			},
		},
	}
}

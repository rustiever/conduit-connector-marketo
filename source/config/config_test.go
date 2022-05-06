// Copyright © 2022 Meroxa, Inc.
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

package config

import (
	"context"
	"reflect"
	"testing"
	"time"

	globalConfig "github.com/rustiever/conduit-connector-marketo/config"
)

func TestParseGlobalConfig(t *testing.T) {
	var configTests = []struct {
		name        string
		wantErr     bool
		in          map[string]string
		expectedCon SourceConfig
	}{
		{
			name:        "Empty Input",
			wantErr:     true,
			in:          map[string]string{},
			expectedCon: SourceConfig{},
		},
		{
			name:    "Empty Polling period",
			wantErr: false,
			in: map[string]string{
				"client_id":      "client_id",
				"client_secret":  "client_secret",
				"endpoint":       "https://xxx-xxx-xxx.mktorest.com",
				"polling_period": "",
			},
			expectedCon: SourceConfig{
				Config: globalConfig.Config{
					ClientID:     "client_id",
					ClientSecret: "client_secret",
					Endpoint:     "https://xxx-xxx-xxx.mktorest.com",
				},
				PollingPeriod: time.Duration(1 * time.Minute),
			},
		},
		{
			name:    "Polling period which is not parsed",
			wantErr: true,
			in: map[string]string{
				"client_id":     "client_id",
				"client_secret": "client_secret",
				"endpoint":      "https://xxx-xxx-xxx.mktorest.com",
				"pollingPeriod": "not parsed",
			},
			expectedCon: SourceConfig{},
		},
		{
			name:    "Polling period which is negative",
			wantErr: true,
			in: map[string]string{
				"client_id":     "client_id",
				"client_secret": "client_secret",
				"endpoint":      "https://xxx-xxx-xxx.mktorest.com",
				"pollingPeriod": "-1m",
			},
			expectedCon: SourceConfig{},
		},
	}

	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			actualCon, err := ParseSourceConfig(context.Background(), tt.in)
			if (!(err != nil && tt.wantErr)) && (err != nil || tt.wantErr) {
				t.Errorf("Expected want error is %v but got parse error as : %v ", tt.wantErr, err)
			}
			if !reflect.DeepEqual(tt.expectedCon, actualCon) {
				t.Errorf("Expected Config %v doesn't match with actual config %v ", tt.expectedCon, actualCon)
			}
		})
	}
}

func TestGetOrderedFields(t *testing.T) {
	var configTests = []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "Nil Input",
			input:    nil,
			expected: []string{"id", "createdAt", "updatedAt", "firstName", "lastName", "email"},
		},
		{
			name:     "Empty Input",
			input:    []string{},
			expected: []string{"id", "createdAt", "updatedAt", "firstName", "lastName", "email"},
		},
		{
			name:     "With fields",
			input:    []string{"email", "firstName"},
			expected: []string{"id", "createdAt", "updatedAt", "email", "firstName"},
		},
	}

	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getOrderedFields(tt.input)
			if !reflect.DeepEqual(tt.expected, actual) {
				t.Errorf("Expected %v doesn't match with actual %v ", tt.expected, actual)
			}
		})
	}
}
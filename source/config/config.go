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
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rustiever/conduit-connector-marketo/config"
)

const (
	// Marketo CDC polling period
	ConfigKeyPollingPeriod = "pollingPeriod"

	// Fields to retrieve from Marketo database
	ConfigKeyFields = "fields"

	// DefaultPollingPeriod is the value assumed for the pooling period when the
	// config omits the polling period parameter
	DefaultPollingPeriod = "1m"
)

// SourceConfig represents source configuration with GCS configurations
type SourceConfig struct {
	config.Config
	PollingPeriod time.Duration
	Fields        []string
}

// ParseSourceConfig attempts to parse the configurations into a SourceConfig struct that Source could utilize
func ParseSourceConfig(ctx context.Context, cfg map[string]string) (SourceConfig, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "ParseSourceConfig").Logger()
	logger.Trace().Msg("Start Parsing the Config")

	globalConfig, err := config.ParseGlobalConfig(ctx, cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Parsing the Global Config")
		return SourceConfig{}, err
	}

	pollingPeriodString, exists := cfg[ConfigKeyPollingPeriod]
	if !exists || pollingPeriodString == "" {
		pollingPeriodString = DefaultPollingPeriod
	}
	pollingPeriod, err := time.ParseDuration(pollingPeriodString)
	if err != nil {
		return SourceConfig{}, fmt.Errorf(
			"%q config value should be a valid duration",
			ConfigKeyPollingPeriod,
		)
	}
	if pollingPeriod <= 0 {
		return SourceConfig{}, fmt.Errorf(
			"%q config value should be positive, got %s",
			ConfigKeyPollingPeriod,
			pollingPeriod,
		)
	}

	var fields []string
	if cfg[ConfigKeyFields] == "" {
		fields = getOrderedFields(nil)
	} else {
		fields = getOrderedFields(strings.Split(cfg[ConfigKeyFields], ","))
	}

	logger.Trace().Msg("Start Parsing the Config")
	return SourceConfig{
		Config:        globalConfig,
		PollingPeriod: pollingPeriod,
		Fields:        fields,
	}, nil
}

// returns default fields if no fields are specified and if some specfied prepends required fields
func getOrderedFields(fields []string) []string {
	if fields == nil || len(fields) == 0 {
		return []string{"id", "createdAt", "updatedAt", "firstName", "lastName", "email"}
	}
	var tempFields = []string{"id", "createdAt", "updatedAt"}
	for _, field := range fields {
		if field == "id" || field == "createdAt" || field == "updatedAt" {
			continue
		}
		tempFields = append(tempFields, field)
	}
	return tempFields
}

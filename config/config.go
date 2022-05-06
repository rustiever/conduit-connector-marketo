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
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// ID: Marketo client ID
	ClientID = "client_id"
	// Secret: Marketo client secret
	ClientSecret = "client_secret"
	// ClientEndpoint: https://xxx-xxx-xxx.mktorest.com
	ClientEndpoint = "endpoint"

)

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

// Config represents configuration needed for Marketo

type Config struct {
	ClientID     string
	ClientSecret string
	Endpoint     string
}

// Parse attempts to parse plugins.Config into a Config struct
func ParseGlobalConfig(ctx context.Context, cfg map[string]string) (Config, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "ParseGlobalConfig").Logger()
	logger.Trace().Msg("Started Parsing the config")

	err := checkEmpty(cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Check Empty")
		return Config{}, err
	}

	clientID, ok := cfg[ClientID]
	if !ok {
		err := requiredConfigErr(ClientID)
		logger.Error().Stack().Err(err).Msgf("Error While Parsing %s", ClientID)
		return Config{}, err
	}

	clientSecret, ok := cfg[ClientSecret]
	if !ok {
		err := requiredConfigErr(ClientSecret)
		logger.Error().Stack().Err(err).Msgf("Error While Parsing %s", ClientSecret)
		return Config{}, err
	}

	endpoint, ok := cfg[ClientEndpoint]
	if !ok {
		err := requiredConfigErr(ClientEndpoint)
		logger.Error().Stack().Err(err).Msgf("Error While Parsing %s", ClientEndpoint)
		return Config{}, err
	}

	logger.Trace().Msg("Successfully Parsed the config")
	config := Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Endpoint:     endpoint,
	}

	return config, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}

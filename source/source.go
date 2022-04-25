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
package source

import (
	"context"

	"github.com/SpeakData/minimarketo"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rustiever/conduit-connector-marketo/source/config"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource
	config   config.SourceConfig
	client   minimarketo.Client
	iterator Iterator
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Configure").Logger()
	logger.Trace().Msg("Starting Configuring the Source Connector...")

	sourceConfig, err := config.ParseSourceConfig(ctx, cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While parsing the Source Config")
		return err
	}
	s.config = sourceConfig
	logger.Trace().Msg("Successfully Configured the Source Connector")
	return err
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Open").Logger()
	logger.Trace().Msg("Starting Open the Source Connector...")

	config := minimarketo.ClientConfig{
		ID:       s.config.ClientID,
		Secret:   s.config.ClientSecret,
		Endpoint: s.config.Endpoint,
		Debug:    true,
		// Debug: false,
	}
	var err error
	s.client, err = minimarketo.NewClient(config)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Creating the Client")
		return err
	}
	logger.Trace().Msg("Successfully Created the Source Connector")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	return s.iterator.Next(ctx)
}

func (s *Source) Ack(ctx context.Context, pos sdk.Position) error {
	return nil
}

func (s *Source) TearDown(ctx context.Context) error {
	return nil
}

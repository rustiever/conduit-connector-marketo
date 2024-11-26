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
	"time"

	"github.com/rustiever/conduit-connector-marketo/config"
)

const (
	// DefaultPollingPeriod is the value assumed for the pooling period when the
	// config omits the polling period parameter
	DefaultPollingPeriod = time.Minute
)

//go:generate paramgen -output=paramgen.go SourceConfig

// SourceConfig represents source configuration with GCS configurations
type SourceConfig struct {
	config.Config
	// PollingPeriod is the polling time for CDC mode. Less than 10s is not recommended.
	PollingPeriod time.Duration `json:"pollingPeriod"`
	// SnapshotInitialDate is the date from which the snapshot iterator initially starts getting records.
	SnapshotInitialDate string `json:"snapshotInitialDate"`
	// Fields are comma seperated fields to fetch from Marketo Leads.
	Fields []string `json:"fields"`
}

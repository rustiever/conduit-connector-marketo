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
package marketo

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	globalConfig "github.com/rustiever/conduit-connector-marketo/config"
	sourceConfig "github.com/rustiever/conduit-connector-marketo/source/config"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "Marketo",
		Summary: "A Adobe Marketo source connector, which pulls data from a given Marketo instance.",
		Version: "v0.1.0",
		Author:  "Sharan",
		SourceParams: map[string]sdk.Parameter{
			globalConfig.ClientID: {
				Required:    true,
				Default:     "",
				Description: "The client ID for the Marketo instance.",
			},
			globalConfig.ClientSecret: {
				Required:    true,
				Default:     "",
				Description: "The client secret for the Marketo instance.",
			},
			globalConfig.Endpoint: {
				Required:    true,
				Default:     "",
				Description: "The endpoint for the Marketo instance.",
			},
			sourceConfig.ConfigKeyPollingPeriod: {
				Required:    false,
				Default:     "1m",
				Description: "The polling period CDC mode.",
			},
		},
	}
}

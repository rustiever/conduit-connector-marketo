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

// Config represents configuration needed for Marketo.
type Config struct {
	// ClientID is the Client ID for Marketo Instance.
	ClientID string `json:"clientID" validate:"required"`
	// ClientSecret is the Client Secret for Marketo Instance.
	ClientSecret string `json:"clientSecret" validate:"required"`
	// ClientEndpoint is the Endpoint for Marketo Instance.
	ClientEndpoint string `json:"clientEndpoint" validate:"required"`
}

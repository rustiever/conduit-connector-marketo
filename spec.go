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
)

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "marketo",
		Summary:     "A Adobe Marketo source connector, which syncs Leads from a given Marketo instance.",
		Description: "Marketo source connector connects to Marketo instance through the REST API with provided configuration, using `clientID` and `clientSecret`. Once connector is started `Configure` method is called to parse configurations and validate them. After that `Open` method is called to establish connection to Marketo instance with provided position. Once connection is established `Read` method is called which calls current iterator's `Next` method to fetch next record. `Teardown` is called when connector is stopped.",
		Version:     version,
		Author:      "Sharan",
	}
}

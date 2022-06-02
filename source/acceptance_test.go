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
package source_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"

	marketo "github.com/rustiever/conduit-connector-marketo"
	"github.com/rustiever/conduit-connector-marketo/source"
	"github.com/rustiever/conduit-connector-marketo/source/iterator"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"go.uber.org/goleak"
)

var (
	startTime time.Time
)

func TestAcceptance(t *testing.T) {
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		Config: AcceptanceSourceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: marketo.Specification,
				NewSource:        source.NewSource,
			},
			SourceConfig: getConfigs(),
			GoleakOptions: []goleak.Option{
				goleak.IgnoreCurrent(),
				goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				// external dependency - minimarketo
				goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				goleak.IgnoreTopFunction("net/http.(*persistConn).roundTrip"),
				goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"),
			},
			BeforeTest: func(t *testing.T) {
				iterator.InitialDate = time.Now().UTC()
				startTime = time.Now().UTC()
			},
			AfterTest: func(t *testing.T) {
				t.Cleanup(func() {
					err := cleanUp(client, startTime)
					if err != nil {
						t.Error(err)
					}
				})
			},
		},
	},
	)
}

// AcceptanceTestDriver implements sdk.AcceptanceTestDriver
type AcceptanceTestDriver struct {
	Config AcceptanceSourceTestDriverConfig
}

// AcceptanceSourceTestDriverConfig contains the configuration for
// AcceptanceTestDriver.
type AcceptanceSourceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector sdk.Connector

	// SourceConfig config for source
	SourceConfig map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest func(t *testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest func(t *testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip is a slice of regular expressions used to identify tests that should
	// be skipped. The full test name will be matched against all regular
	// expressions and the test will be skipped if a match is found.
	Skip []string
}

func (d AcceptanceTestDriver) DestinationConfig(*testing.T) map[string]string {
	return map[string]string{}
}
func (d AcceptanceTestDriver) Connector() sdk.Connector {
	return d.Config.Connector
}

func (d AcceptanceTestDriver) SourceConfig(*testing.T) map[string]string {
	return d.Config.SourceConfig
}

func (d AcceptanceTestDriver) BeforeTest(t *testing.T) {
	// before test check if the test should be skipped
	d.Skip(t)

	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t)
	}
}

func (d AcceptanceTestDriver) AfterTest(t *testing.T) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t)
	}
}

func (d AcceptanceTestDriver) Skip(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skipf("caller requested to skip tests that match the regex %q", skipRegex.String())
		}
	}
}

func (d AcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

// WriteToSource writes data for source to pull data from
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	var err error
	is := is.New(t)
	client, err := getClient()
	is.NoErr(err)
	leads, err := addLeads(client, len(records))
	is.NoErr(err)
	var emailIDs = make([]string, 0)
	for _, v := range leads {
		emailIDs = append(emailIDs, v["email"].(string))
	}
	records, err = writeRecords(client, emailIDs)
	if err != nil {
		t.Error(err)
	}

	return records
}

func (d AcceptanceTestDriver) ReadFromDestination(*testing.T, []sdk.Record) []sdk.Record {
	return []sdk.Record{}
}

func writeRecords(client Client, emailIDs []string) ([]sdk.Record, error) {
	data, err := client.filterLeads("email", emailIDs)
	if err != nil {
		return nil, err
	}
	var record []map[string]interface{}
	err = json.Unmarshal(*data, &record)
	if err != nil {
		return nil, err
	}
	var records = make([]sdk.Record, 0)
	for _, v := range record {
		id := int(v["id"].(float64))
		data, err = client.getLeadByID(id, Fields)
		if err != nil {
			return nil, err
		}
		var rec []map[string]interface{}
		err = json.Unmarshal(*data, &rec)
		if err != nil {
			return nil, err
		}
		rec[0]["id"] = fmt.Sprint(id)
		r, err := prepareRecord(rec[0])
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, err
}

func prepareRecord(data map[string]interface{}) (sdk.Record, error) {
	createdAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", data["createdAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing createdAt %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", data["updatedAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing updatedAt %w", err)
	}
	position := position.Position{
		Key:       fmt.Sprint(data["id"]),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Type:      position.TypeSnapshot,
	}
	pos, err := position.ToRecordPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error converting position to record position %w", err)
	}
	rec := sdk.Record{
		Payload: sdk.StructuredData(data),
		Metadata: map[string]string{
			"id":        position.Key,
			"createdAt": createdAt.Format(time.RFC3339),
			"updatedAt": updatedAt.Format(time.RFC3339),
		},
		Position: pos,
		Key:      sdk.RawData(position.Key),
	}

	return rec, nil
}

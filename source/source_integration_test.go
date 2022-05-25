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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SpeakData/minimarketo"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/goombaio/namegenerator"
	"github.com/rustiever/conduit-connector-marketo/config"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	sourceConfig "github.com/rustiever/conduit-connector-marketo/source/config"
	"github.com/rustiever/conduit-connector-marketo/source/iterator"
	"github.com/rustiever/conduit-connector-marketo/source/position"
)

var (
	ClinetID       = os.Getenv("MARKETO_CLIENT_ID")
	ClientSecret   = os.Getenv("MARKETO_CLIENT_SECRET")
	ClientEndpoint = os.Getenv("MARKETO_CLIENT_ENDPOINT")
	fields         = []string{"firstName", "lastName", "email", "createdAt", "updatedAt"} // fields to be returned by the API
)

func TestSource_SuccessfullSnapshot(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := AddLeads(client, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	src := &Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err = configAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_SnapshotRestart(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := AddLeads(client, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	src := &Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	pos, err := json.Marshal(position.Position{
		Key:       "1",
		CreatedAt: startTime,
		UpdatedAt: startTime,
		Type:      position.TypeSnapshot,
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	err = configAndOpen(ctx, src, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_EmptyDatabase(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	ctx := context.Background()
	src := &Source{}
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
	_, _ = src.Read(ctx)
	time.Sleep(time.Second * 1)
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := AddLeads(client, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_NonExistentDatabase(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	cfg := getConfigs()
	cfg[config.ClientID] = "non-existent"
	cfg[config.ClientSecret] = "non-existent"
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	err = src.Open(ctx, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSource_CDC_ReadRecordsUpdate(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := AddLeads(client, 1)
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := src.Read(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	assert(t, &rec, testLeads[0])
	updatedLeads, err := UpdateLeads(client, testLeads[0]["email"].(string))
	if err != nil {
		t.Fatal(err)
	}
	rec = nextRecord(ctx, src, t)
	var record map[string]interface{}
	err = json.Unmarshal(rec.Payload.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if record["email"] != updatedLeads["email"] && record["lastName"] != updatedLeads["lastName"] {
		t.Errorf("expected %v, got %v", updatedLeads, record)
	}
}

func TestCDC_Delete(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	ctx := context.Background()
	src := &Source{}
	defer func() {
		_ = src.TearDown(ctx)
	}()
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	err = configAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	startTime := time.Now().UTC()
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := AddLeads(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	var rec sdk.Record
	for _, lead := range testLeads {
		rec = nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
	var record map[string]interface{}
	err = json.Unmarshal(rec.Payload.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	leadID := record["id"].(string)
	err = client.DeleteLeadsByIDs([]string{leadID})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	rec = nextRecord(ctx, src, t)
	err = json.Unmarshal(rec.Key.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if record["id"].(string) != leadID {
		t.Errorf("expected %v, got %v", leadID, record["id"])
	}
}

func TestSource_CDC_ReadRecordsInsertAfterTeardown(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &Source{}
	ctx := context.Background()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := AddLeads(client, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	var rec sdk.Record
	for _, lead := range testLeads {
		rec = nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	lastPosition := rec.Position
	_ = src.TearDown(ctx)
	src1 := &Source{}
	defer func() {
		_ = src1.TearDown(ctx)
	}()
	err = configAndOpen(ctx, src1, lastPosition)
	if err != nil {
		t.Fatal(err)
	}
	testLeads, err = AddLeads(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	rec = nextRecord(ctx, src1, t)
	assert(t, &rec, testLeads[0])
}

func TestOpenSource_FailsParsePosition(t *testing.T) {
	ctx := context.Background()
	source := &Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()
	err := source.Configure(ctx, getConfigs())
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, []byte("Invalid Position"))
	expectedErr := "invalid character 'I' looking for beginning of value"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestOpenSource_InvalidPositionType(t *testing.T) {
	ctx := context.Background()
	source := &Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()
	err := source.Configure(ctx, getConfigs())
	if err != nil {
		t.Fatal(err)
	}
	p, err := json.Marshal(position.Position{
		Key:       "key",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Type:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, p)
	expectedErr := "couldn't create a combined iterator: invalid position type (2)"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := &Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)
	}
	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

// returns configs for testing.
func getConfigs() map[string]string {
	cfg := map[string]string{}
	cfg[config.ClientID] = ClinetID
	cfg[config.ClientSecret] = ClientSecret
	cfg[config.ClientEndpoint] = ClientEndpoint
	cfg[sourceConfig.ConfigKeyPollingPeriod] = "10s"
	return cfg
}

// returns new client.
func getClient() (marketoclient.Client, error) {
	client, err := marketoclient.NewClient(minimarketo.ClientConfig{
		ID:       ClinetID,
		Secret:   ClientSecret,
		Endpoint: ClientEndpoint,
	})
	if err != nil {
		return marketoclient.Client{}, err
	}
	return client, nil
}

// asserts actual record against expected lead.
func assert(t *testing.T, actual *sdk.Record, expected map[string]interface{}) {
	var record map[string]interface{}
	err := json.Unmarshal(actual.Payload.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if expected["firstName"] != record["firstName"] {
		t.Errorf("expected firstName %v, got %v", expected["firstName"], record["firstName"])
	}
	if expected["lastName"] != record["lastName"] {
		t.Errorf("expected lastName %v, got %v", expected["lastName"], record["lastName"])
	}
	if expected["email"] != record["email"] {
		t.Errorf("expected email %v, got %v", expected["email"], record["email"])
	}
}

// generates a n number of leads and adds them to the database, also returns leads.
func AddLeads(client marketoclient.Client, count int) ([]map[string]interface{}, error) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	var leads []map[string]interface{}
	for i := 0; i < count; i++ {
		firstname := nameGenerator.Generate()
		leads = append(leads, map[string]interface{}{
			"firstName": firstname,
			"lastName":  nameGenerator.Generate(),
			"email":     firstname + "@meroxa.com",
		})
	}
	err := client.CreateOrUpdateLeads(marketoclient.CreateOnly, leads)
	if err != nil {
		return nil, err
	}
	return leads, nil
}

// updates the leads for given LeadID
func UpdateLeads(client marketoclient.Client, emailID string) (map[string]interface{}, error) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	leads := map[string]interface{}{
		"lastName": nameGenerator.Generate(),
		"email":    emailID,
	}
	err := client.CreateOrUpdateLeads(marketoclient.UpdateOnly, []map[string]interface{}{leads})
	if err != nil {
		return nil, err
	}
	return leads, nil
}

// gets next record from the source
func nextRecord(ctx context.Context, src *Source, t *testing.T) (rec sdk.Record) {
	var err error
	for {
		rec, err = src.Read(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			continue
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		break
	}
	return
}

// deletes all leads from marketo API
func cleanUp(client marketoclient.Client, sinceTime time.Time) error {
	token, err := client.GetNextPageToken(sinceTime)
	if err != nil {
		return err
	}
	res, err := client.GetLeadChanges(token, fields)
	if err != nil {
		return err
	}
	if len(res.Result) == 0 {
		return nil
	}
	leadResult := []map[string]interface{}{}
	err = json.Unmarshal(res.Result, &leadResult)
	if err != nil {
		return err
	}
	var leadIDs = make([]string, 0)
	for _, data := range leadResult {
		leadIDs = append(leadIDs, fmt.Sprint(data["leadId"].(float64)))
	}
	if len(leadIDs) == 0 {
		return nil
	}
	err = client.DeleteLeadsByIDs(leadIDs)
	if err != nil {
		return err
	}
	return nil
}

// configures the source with the given configs and establishes a connection to Marketo
func configAndOpen(ctx context.Context, s *Source, pos sdk.Position) error {
	err := s.Configure(ctx, getConfigs())
	if err != nil {
		return err
	}
	err = s.Open(ctx, pos)
	if err != nil {
		return err
	}
	return nil
}

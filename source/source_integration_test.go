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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rustiever/conduit-connector-marketo/config"
	"github.com/rustiever/conduit-connector-marketo/source"
	"github.com/rustiever/conduit-connector-marketo/source/iterator"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"github.com/rustiever/conduit-connector-marketo/source/util"
)

func TestSource_SuccessfullSnapshot(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := util.AddLeads(client, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	src := &source.Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err = util.ConfigAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := util.NextRecord(ctx, src, t)
		util.Assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_SnapshotRestart(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := util.AddLeads(client, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	src := &source.Source{}
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
	err = util.ConfigAndOpen(ctx, src, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := util.NextRecord(ctx, src, t)
		util.Assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_EmptyDatabase(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &source.Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := util.ConfigAndOpen(ctx, src, nil)
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
	src := &source.Source{}
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := util.ConfigAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
	_, _ = src.Read(ctx)
	time.Sleep(time.Second * 1)
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := util.AddLeads(client, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, lead := range testLeads {
		rec := util.NextRecord(ctx, src, t)
		util.Assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_NonExistentDatabase(t *testing.T) {
	iterator.InitialDate = time.Now().UTC()
	src := &source.Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	cfg := util.GetConfigs()
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
	src := &source.Source{}
	ctx := context.Background()
	defer func() {
		_ = src.TearDown(ctx)
	}()
	err := util.ConfigAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := util.AddLeads(client, 1)
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
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
	util.Assert(t, &rec, testLeads[0])
	updatedLeads, err := util.UpdateLeads(client, testLeads[0]["email"].(string))
	if err != nil {
		t.Fatal(err)
	}
	rec = util.NextRecord(ctx, src, t)
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
	src := &source.Source{}
	defer func() {
		_ = src.TearDown(ctx)
	}()
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	err = util.ConfigAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	startTime := time.Now().UTC()
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := util.AddLeads(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	var rec sdk.Record
	for _, lead := range testLeads {
		rec = util.NextRecord(ctx, src, t)
		util.Assert(t, &rec, lead)
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
	rec = util.NextRecord(ctx, src, t)
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
	src := &source.Source{}
	ctx := context.Background()
	err := util.ConfigAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := util.GetClient()
	if err != nil {
		t.Fatal(err)
	}
	startTime := time.Now().UTC()
	testLeads, err := util.AddLeads(client, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := util.CleanUp(client, startTime)
		if err != nil {
			t.Error(err)
		}
	})
	var rec sdk.Record
	for _, lead := range testLeads {
		rec = util.NextRecord(ctx, src, t)
		util.Assert(t, &rec, lead)
	}
	lastPosition := rec.Position
	_ = src.TearDown(ctx)
	src1 := &source.Source{}
	defer func() {
		_ = src1.TearDown(ctx)
	}()
	err = util.ConfigAndOpen(ctx, src1, lastPosition)
	if err != nil {
		t.Fatal(err)
	}
	testLeads, err = util.AddLeads(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	rec = util.NextRecord(ctx, src1, t)
	util.Assert(t, &rec, testLeads[0])
}

func TestOpenSource_FailsParsePosition(t *testing.T) {
	ctx := context.Background()
	source := &source.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()
	err := source.Configure(ctx, util.GetConfigs())
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
	source := &source.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()
	err := source.Configure(ctx, util.GetConfigs())
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
	con := &source.Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)
	}
	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

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
package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"gopkg.in/tomb.v2"
)

const (
	ACTIVITY_TYPE_ID_NEW_LEAD         = 12
	ACTIVITY_TYPE_ID_CHANGE_DATA_VALE = 13
)

type CDCIterator struct {
	client        *marketoclient.Client
	buffer        chan Record
	ticker        *time.Ticker
	pollingPeriod time.Duration
	fields        []string
	tomb          *tomb.Tomb
	lastModified  time.Time
}

func NewCDCIterator(ctx context.Context, client *marketoclient.Client, pollingPeriod time.Duration, fields []string, lastModifiedTime time.Time) (*CDCIterator, error) {
	iterator := &CDCIterator{
		client:       client,
		buffer:       make(chan Record, 1),
		ticker:       time.NewTicker(pollingPeriod), // TODO revert polling period
		tomb:         &tomb.Tomb{},
		fields:       fields,
		lastModified: lastModifiedTime,
	}
	iterator.tomb.Go(func() error {
		return iterator.poll(ctx)
	})
	return iterator, nil
}

func (c *CDCIterator) poll(ctx context.Context) error {
	defer close(c.buffer)
	for {
		select {
		case <-c.tomb.Dying():
			return c.tomb.Err()
		case <-c.ticker.C:
			err := c.flushLatestLeads(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (c *CDCIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Method", "Has Next").Logger()
	// logger.Trace().Msg("Starting the Combined Iterator Next")
	logger.Debug().Msg("Checking if the CDC iterator has next")
	return len(c.buffer) > 0 || !c.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-c.buffer:
		return c.prepareRecord(r)
	case <-c.tomb.Dead():
		return sdk.Record{}, c.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (c *CDCIterator) Stop() {
	// stop the two goRoutines
	c.ticker.Stop()
	c.tomb.Kill(errors.New("cdc iterator is stopped"))
}

func (c *CDCIterator) prepareRecord(r Record) (sdk.Record, error) {
	key := strconv.Itoa(r.id)
	if r.deleted {
		position := position.Position{
			Type:      position.TypeCDC,
			Key:       key,
			CreatedAt: time.Now().UTC(),
		}
		pos, err := position.ToRecordPosition()
		if err != nil {
			return sdk.Record{}, err
		}
		return sdk.Record{
			Key: sdk.StructuredData{
				"id": key,
			},
			Position: pos,
			Metadata: map[string]string{
				"action": "delete",
			},
			CreatedAt: time.Now().UTC(),
		}, nil
	}
	createdAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", r.data["createdAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing createdAt %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", r.data["updatedAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing updatedAt %w", err)
	}
	position, _ := position.Position{
		Type:      position.TypeCDC,
		Key:       key,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}.ToRecordPosition()
	rec := sdk.Record{
		Payload: sdk.StructuredData(r.data),
		Metadata: map[string]string{
			"id":        key,
			"createdAt": createdAt.UTC().Format(time.RFC3339),
			"updatedAt": updatedAt.UTC().Format(time.RFC3339),
		},
		Position: position,
		Key: sdk.StructuredData{
			"id": key,
		},
	}

	return rec, nil
}

func (c *CDCIterator) flushLatestLeads(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Method", "getLatestLeads").Logger()
	logger.Trace().Msg("Starting the getLatestLeads")
	token, err := c.client.GetNextPageToken(c.lastModified)
	c.lastModified = time.Now().UTC()
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the next page token")
		return err
	}
	changedLeadIds, err := c.GetChangedLeadsIDs(ctx, token)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the changed leads")
		return err
	}

	deletedLeadIds, err := c.GetDeletedLeadsIDs(ctx, token)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the deleted leads")
		return err
	}
	for leadId := range changedLeadIds {
		res, err := c.client.GetLeadById(ctx, leadId, c.fields)
		if err != nil {
			logger.Error().Err(err).Msg("Error while getting the lead")
			return err
		}
		dataMap := make([]map[string]interface{}, 0)
		err = json.Unmarshal(*res, &dataMap)
		if err != nil {
			logger.Error().Err(err).Msg("Error while unmarshalling the lead")
			return err
		}
		for _, data := range dataMap {
			c.buffer <- Record{
				id:      leadId,
				deleted: false,
				data:    data,
			}
		}
	}

	for leadId := range deletedLeadIds {
		c.buffer <- Record{
			id:      leadId,
			deleted: true,
			data:    nil,
		}
	}

	return nil
}

type Record struct {
	id      int
	data    map[string]interface{}
	deleted bool
}

func (c *CDCIterator) GetDeletedLeadsIDs(ctx context.Context, token string) (map[int]struct{}, error) {
	response, err := c.client.GetDeletedLeads(ctx, token)
	if err != nil {
		return nil, err
	}
	if len(*response) == 0 {
		return nil, nil
	}
	var deletedLeadResults []map[string]interface{}
	err = json.Unmarshal(*response, &deletedLeadResults)
	if err != nil {
		return nil, err
	}
	var leadIds = make(map[int]struct{})
	for _, deletedLeadResult := range deletedLeadResults {
		var id = int(deletedLeadResult["leadId"].(float64))
		leadIds[id] = struct{}{}

	}
	return leadIds, nil
}

func (c *CDCIterator) GetChangedLeadsIDs(ctx context.Context, token string) (map[int]struct{}, error) {
	var leadIds = make(map[int]struct{}) // using map to avoid duplicates
	moreResult := true
	for moreResult {
		response, err := c.client.GetLeadChanges(ctx, token, c.fields)
		if err != nil {
			return nil, err
		}
		if len(response.Result) == 0 {
			return nil, nil
		}
		moreResult = response.MoreResult
		token = response.NextPageToken
		var leadChangeResults []map[string]interface{}
		err = json.Unmarshal(response.Result, &leadChangeResults)
		if err != nil {
			return nil, err
		}
		for _, leadChangeResult := range leadChangeResults {
			var activityTypeId = leadChangeResult["activityTypeId"].(float64)
			if activityTypeId == ACTIVITY_TYPE_ID_NEW_LEAD || activityTypeId == ACTIVITY_TYPE_ID_CHANGE_DATA_VALE {
				var id = int(leadChangeResult["leadId"].(float64))
				leadIds[id] = struct{}{}
			}
		}
	}
	return leadIds, nil
}

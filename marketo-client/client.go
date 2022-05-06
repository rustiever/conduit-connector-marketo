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
package marketoclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/SpeakData/minimarketo"
)

var (
	ErrEnqueueLimit = errors.New("Enqueue limit reached")
	ErrZeroRecords  = errors.New("No records found")
	ErrCannotCancel = errors.New("Cannot cancel export, since it is already in completed state")
)

type Client struct {
	minimarketo.Client
}

func NewClient(config minimarketo.ClientConfig) (Client, error) {
	client, err := minimarketo.NewClient(config)
	if err != nil {
		return Client{}, err
	}
	return Client{client}, nil
}

func (c Client) CreateExportLeads(ctx context.Context, fields []string, startDate string, endDate string) (string, error) {
	reqBody, err := json.Marshal(map[string]interface{}{
		"filter": map[string]interface{}{
			"createdAt": map[string]string{
				"startAt": startDate,
				"endAt":   endDate,
			},
		},
		"fields": fields,
	})
	if err != nil {
		return "", err
	}
	path := "/bulk/v1/leads/export/create.json"
	response, err := c.Post(path, reqBody)
	if err != nil {
		return "", err
	}
	if !response.Success {
		return "", fmt.Errorf("%+v", response.Errors)
	}
	var result []CreateExportResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return "", err
	}
	if len(result) != 1 {
		return "", fmt.Errorf("Unexpected response from Marketo rest API:%+v", result)
	}
	return result[0].ExportID, nil
}

type CreateExportResult struct {
	ExportID  string    `json:"exportId"`
	Format    string    `json:"format"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

func (c Client) EnqueueExportLeads(ctx context.Context, exportID string) (string, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/enqueue.json", exportID)
	response, err := c.Post(path, nil)
	if err != nil {
		return "", err
	}

	if !response.Success {
		if response.Errors[0].Code == "1029" {
			return "", ErrEnqueueLimit
		}
		return "", fmt.Errorf("%+v", response.Errors)
	}
	return exportID, nil
}

func (c Client) StatusOfExportLeads(ctx context.Context, exportID string) (StatusOfExportResult, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/status.json", exportID)
	response, err := c.Get(path)
	if err != nil {
		return StatusOfExportResult{}, err
	}
	if !response.Success {
		return StatusOfExportResult{}, fmt.Errorf("%+v", response.Errors)
	}
	var result []StatusOfExportResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return StatusOfExportResult{}, err
	}
	if len(result) != 1 {
		return StatusOfExportResult{}, fmt.Errorf("Unexpected response from Marketo rest API:%+v", result)
	}
	return result[0], nil
}

type StatusOfExportResult struct {
	ExportID        string    `json:"exportId"`
	Format          string    `json:"format"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"createdAt"`
	QueuedAt        time.Time `json:"queuedAt"`
	StartedAt       time.Time `json:"startedAt"`
	FinishedAt      time.Time `json:"finishedAt"`
	NumberOfRecords int       `json:"numberOfRecords"`
	FileSize        int       `json:"fileSize"`
	FileChecksum    string    `json:"fileChecksum"`
}

func (c Client) CancelExportLeads(ctx context.Context, exportID string) error {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/cancel.json", exportID)
	response, err := c.Post(path, nil)
	if err != nil {
		return err
	}
	if !response.Success {
		if response.Errors[0].Code == "1029" {
			return ErrCannotCancel
		}
		return fmt.Errorf("%+v", response.Errors)
	}
	return nil
}

func (c Client) FileExportLeads(endpoint string, exportID string) (*[]byte, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/file.json", exportID)

	req, err := http.NewRequest("GET", endpoint+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer"+c.getAuthToken())
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &body, nil
}

func (c Client) getAuthToken() string {
	if c.GetTokenInfo().Expires.Before(time.Now().UTC()) {
		_, _ = c.RefreshToken()
	}
	return c.GetTokenInfo().Token
}

func (c Client) GetAllFolders(maxDepth int) ([]FolderResult, error) {
	var folderResult []FolderResult
	path := fmt.Sprintf("/rest/asset/v1/folders.json?maxDepth=%v", maxDepth)
	res, err := c.Get(path)
	if err != nil {
		return []FolderResult{}, err
	}
	err = json.Unmarshal(res.Result, &folderResult)
	if err != nil {
		return []FolderResult{}, err
	}
	return folderResult, nil
}

type FolderResult struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	CreatedAt   string      `json:"createdAt"`
	UpdatedAt   string      `json:"updatedAt"`
	URL         interface{} `json:"url"`
	FolderID    struct {
		ID   int    `json:"id"`
		Type string `json:"type"`
	} `json:"folderId"`
}

func (c Client) GetNextPageToken(sinceTime time.Time) (string, error) {
	formattedTime := sinceTime.UTC().Format(time.RFC3339)
	path := fmt.Sprintf("/rest/v1/activities/pagingtoken.json?sinceDatetime=%s", formattedTime)
	response, err := c.Get(path)
	if err != nil {
		return "", err
	}
	if !response.Success {
		return "", fmt.Errorf("%+v", response.Errors)
	}
	return response.NextPageToken, nil
}

func (c Client) GetLeadChanges(ctx context.Context, nextPageToken string, fields []string) (*minimarketo.Response, error) {
	path := fmt.Sprintf("/rest/v1/activities/leadchanges.json?nextPageToken=%s&fields=%s", nextPageToken, strings.Join(fields, ","))
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return response, nil
}

func (c Client) GetDeletedLeads(ctx context.Context, nextPageToken string) (*json.RawMessage, error) {
	path := fmt.Sprintf("/rest/v1/activities/deletedleads.json?nextPageToken=%s", nextPageToken)
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return &response.Result, nil
}

func (c Client) GetLeadById(ctx context.Context, id int, fields []string) (*json.RawMessage, error) {
	path := fmt.Sprintf("/rest/v1/lead/%d.json?fields=%s", id, strings.Join(fields, ","))
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}

	return &response.Result, nil
}

func GetDataMap(keys []string, values []string) map[string]interface{} {
	dataMap := make(map[string]interface{})
	for i, key := range keys {
		dataMap[key] = values[i]
	}
	return dataMap
}

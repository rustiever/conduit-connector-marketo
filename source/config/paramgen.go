// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	SourceConfigClientEndpoint      = "clientEndpoint"
	SourceConfigClientID            = "clientID"
	SourceConfigClientSecret        = "clientSecret"
	SourceConfigFields              = "fields"
	SourceConfigPollingPeriod       = "pollingPeriod"
	SourceConfigSnapshotInitialDate = "snapshotInitialDate"
)

func (SourceConfig) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		SourceConfigClientEndpoint: {
			Default:     "",
			Description: "ClientEndpoint is the Endpoint for Marketo Instance.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigClientID: {
			Default:     "",
			Description: "ClientID is the Client ID for Marketo Instance.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigClientSecret: {
			Default:     "",
			Description: "ClientSecret is the Client Secret for Marketo Instance.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigFields: {
			Default:     "id,createdAt,updatedAt,firstName,lastName,email",
			Description: "Fields are comma seperated fields to fetch from Marketo Leads.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigPollingPeriod: {
			Default:     "1m",
			Description: "PollingPeriod is the polling time for CDC mode. Less than 10s is not recommended.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		SourceConfigSnapshotInitialDate: {
			Default:     "",
			Description: "SnapshotInitialDate is the date from which the snapshot iterator initially starts getting records.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
	}
}

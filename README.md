### Conduit Connector for Adobe Marketo
[Conduit](https://conduit.io) for Marketo.  <!-- TODO -->

#### Source
A source connector pulls data from given Marketo Instance and pushes it to downstream resources via Conduit.

#### Destination
Yet to be suported.
<A destination connector pushes data from upstream resources to an external resource via Conduit.>

### How to build?
Run `make build` to build the connector.

### Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

### Configuration

| name | part of | description | required | default value |
|------|---------|-------------|----------|---------------|
|`client_id`|source|The Client ID for Marketo Instance|true| |
|`client_secret`|source|The Client Secret for Marketo Instance|true| |
|`endpoint`|source|The Endpoint for Marketo Instance|true||
|`polling period`|source|Polling time for CDC mode|false|`1m`|

### Known Issues & Limitations
* Bulk API has some Limitations.

### Planned work
- [ ] Removal of external dependency `Minimarketo`
- [ ] Add support for Destination 

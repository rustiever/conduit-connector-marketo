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
#### Snapshot mode:
* In snapshot mode, the total amount of data that you can export from Marketo is limited to 500MB per day unless you have purchased a higher data limit. This 500MB limit resets daily at 12:00AM CST.

### Planned work
- [ ] Add support for Destination 
- [ ] Once 500mb limit is hit, connector should change to CDC Mode

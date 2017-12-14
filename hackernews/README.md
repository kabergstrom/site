Watches the HackerNews API for changes and sends changed objects (posts, comments, users) to a NATS Streaming encoded with protobuf. Also handles requests sent through NATS from other services for specific object IDs, which are fetched using the HackerNews API.
It is recommended to only run one `hackernews` instance per cluster.

### Environment
`hackernews` requires a NATS cluster and a connected NATS Streaming instance.

### Configuration
Configuration is done with environment variables

- `NATS_URL` - OPTIONAL URL used to connect to NATS. Defaults to `nats://localhost:4222`
- `NATS_CLUSTER_ID` - REQUIRED the NATS cluster ID. Must match the ID specified when starting the NATS cluster
- `NATS_CLIENT_ID` - OPTIONAL defaults to `hacker-news-producer`
This repository contains a set of services that serve an API for posts, comments and users that is intended to be used by link aggregators similar to HackerNews. It supports listings like hot/new and is designed to support more third-party data sources. Data is currently supplied by the HackerNews API. 
The services rely on MySQL with the Memcache Plugin for persistent storage and NATS + NATS Streaming to communicate. Cross-service messages are encoded using Protobuf.

- [`api`](api/README.md) is a JSON REST API server that serves objects (posts, comments, users) and listings (hot, new)
- [`hackernews`](hackernews/README.md) is a service that watches the HackerNews API for changes and send these changes to other services through NATS. It also serves requests for HackerNews objects.
- [`nats2db`](nats2db/README.md) reads messages sent by `hackernews` and stores these objects in MySQL
- [`mysql2nats`](mysql2nats/README.md) reads the MySQL binlog for modified objects and send the modified IDs to a NATS subject
- [`ranking`](ranking/README.md) reads the modified IDs sent by `mysql2nats` and maintains the listings of objects (hot, new etc)
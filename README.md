This repository contains a set of services that serves an API for posts, comments and users similar to the HackerNews API as well as uses the HackerNews API to read data. The services rely on MySQL with the Memcache Plugin for persistent storage and NATS + NATS Streaming to communicate.
- [`api`](api/README.md) is a JSON REST API server that serves objects (posts, comments, users) and listings (hot, new)
- [`hackernews`](hackernews/README.md) is a service that watches the HackerNews API for changes and send these changes to other services through NATS. It also serves requests for HackerNews objects.
- [`nats2db`](nats2db/README.md) reads messages sent by `hackernews` and stores these objects in MySQL
- [`mysql2nats`](mysql2nats/README.md) reads the MySQL binlog for modified objects and send the modified IDs to a NATS subject
- [`ranking`](ranking/README.md) reads the modified IDs sent by `mysql2nats` and maintains the listings of objects (hot, new etc)
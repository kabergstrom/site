Reads modified objects sent by `mysql2nats` and maintains listings (hot) based on the object scores. The listings are stored in a MySQL `listing_cache` table and are accessed using the MySQL Memcache Plugin.

### Environment
`ranking` requires a MySQL instance with Memcache plugin enabled. The tables in the `db` folder must be present in the instance and entries for the `object` and `listing_cache` tables must be present in the `innodb_memcache` table to enable `ranking` to access them using the memcache protocol.

### Configuration
Configuration is done with environment variables

- `API_MEMCACHE_ADDRESS` - REQUIRED host + port to the MySQL memcache plugin
- `NATS_CLUSTER_ID` - REQUIRED the NATS cluster ID. Must match the ID specified when starting the NATS cluster
- `NATS_URL` - OPTIONAL URL used to connect to NATS. Defaults to `nats://localhost:4222`
- `NATS_CLIENT_ID` - OPTIONAL defaults to `nats2db`
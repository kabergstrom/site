Reads the MySQL binlog and sends a message to a NATS Streaming subject for every object ID changed.
`mysql2nats` stores the binlog position with the changed object IDs in the NATS Streaming subject and thus supports resuming in case of a crash.
Only one instance of `mysql2nats` should be active per NATS/MySQL instance.

# Environment
`mysql2nats` requires a MySQL instance with the tables in the `db` folder present and a NATS cluster with a NATS Streaming instance connected to the cluster.

# Configuration
`NATS_URL` - OPTIONAL URL used to connect to NATS. Defaults to `nats://localhost:4222`
`NATS_CLUSTER_ID` - REQUIRED the NATS cluster ID. Must match the ID specified when starting the NATS cluster
`NATS_CLIENT_ID` - OPTIONAL defaults to `hacker-news-producer`
`MYSQL_USER` - REQUIRED the MySQL username
`MYSQL_PASSWORD` - REQUIRED the MySQL password
`MYSQL_ADDRESS` - REQUIRED `host:port` for connecting to MySQL
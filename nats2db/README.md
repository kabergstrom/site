Reads objects sent through NATS by the `hackernews` service, generates an internal ID and stores them in a MySQL database. When `nats2db` encounters an object ID reference like a comment or user that is not present in the data store, this object ID is requested from the `hackernews` service by sending a request through NATS.
It is possible to run multiple `nats2db` instances simultaneously but `SNOWFLAKE_SERVER_ID` must be be set to a unique number for each instance and the NATS_CLIENT_ID must be unique.

# Environment
`nats2db` requires a MySQL instance with the tables in the `db` folder present and a NATS cluster with a NATS Streaming instance connected to the cluster.

# Configuration
Configuration is done with environment variables
`NATS_CLUSTER_ID` - REQUIRED the NATS cluster ID. Must match the ID specified when starting the NATS cluster
`MYSQL_DATA_SOURCE_NAME` - REQUIRED the MySQL Data Source Name as defined in the [Go-MySQL-Driver](https://github.com/Go-SQL-Driver/MySQL/#dsn-data-source-name)
`NATS_URL` - OPTIONAL URL used to connect to NATS. Defaults to `nats://localhost:4222`
`NATS_CLIENT_ID` - OPTIONAL defaults to `nats2db`
`SNOWFLAKE_SERVER_ID` - OPTIONAL a number specifying the Snowflake node ID for generating object IDs. When running multiple `nats2db`, this must be unique for each instance
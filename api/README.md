Serves a simple JSON REST API for getting objects and listings.
`api` holds no state and can be scaled horizontally.

# Endpoints
Get multiple objects from a set of object IDs
`POST /object/bulk` 
`{ "ids": [ 1, 2, 3 ] }`

Get the `hot` listing
`GET /hot`

Get a single object from an object ID
`GET /object/{id}`

# Environment
`api` requires a MySQL instance with Memcache plugin enabled. The tables in the `db` folder must be present in the instance and entries for the `object` and `listing_cache` tables must be present in the `innodb_memcache` table to enable `api` to access them using the memcache protocol.

# Configuration
Configuration is done with environment variables
`API_MEMCACHE_ADDRESS` - REQUIRED host + port to the MySQL memcache plugin
`API_SERVER_HOST` - OPTIONAL host for the server to bind on
`API_SERVER_PORT` - REQURIED port for the server to serve requests from
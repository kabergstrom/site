CREATE DATABASE IF NOT EXISTS site;
USE site;

CREATE TABLE IF NOT EXISTS object (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `source` TINYINT UNSIGNED NOT NULL,
    `type` TINYINT UNSIGNED NOT NULL,
    `score` BIGINT NOT NULL,
    `source_score` BIGINT NOT NULL,
    `deleted` BOOLEAN NOT NULL,
    `unixtime` INT NOT NULL,
    `compression` TINYINT UNSIGNED NOT NULL,
    `encoding` TINYINT UNSIGNED NOT NULL,
    `data` BLOB NOT NULL,
    `kids` BLOB NOT NULL,
    `num_kids` INT NOT NULL,
    `version` INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS urls (
    `url_hash` BINARY(32) NOT NULL PRIMARY KEY,
    `url` TEXT NOT NULL,
    `post_id` BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS votes (
    `user_id` BIGINT NOT NULL,
    `post_id` BIGINT NOT NULL,
    `type` TINYINT UNSIGNED NOT NULL, -- vote, report
    `amount` INT NOT NULL, -- vote amount: upvote or downvote. 0 if removed
    PRIMARY KEY(user_id, post_id, type)
);

CREATE TABLE IF NOT EXISTS source_id_to_object_id (
    `source` TINYINT NOT NULL,
    `source_id` VARBINARY(767) NOT NULL,
    `object_id` BIGINT NOT NULL,
    PRIMARY KEY(source, source_id),
    UNIQUE INDEX obj_id(object_id)
);

CREATE TABLE IF NOT EXISTS listing_cache (
    `id` INT NOT NULL,
    `data` BLOB NOT NULL,
    `version` INT NOT NULL,
    PRIMARY KEY(id)
);

INSERT INTO `innodb_memcache`.`containers` (
       `name`, `db_schema`, `db_table`, `key_columns`, `value_columns`,
       `flags`, `cas_column`, `expire_time_column`, `unique_idx_name_on_key`)
       VALUES ('object_data', 'site', 'object', 'id', 'id|source|type|score|source_score|deleted|unixtime|compression|encoding|num_kids|data|kids', 
'0','version','0','PRIMARY');

INSERT INTO `innodb_memcache`.`containers` (
       `name`, `db_schema`, `db_table`, `key_columns`, `value_columns`,
       `flags`, `cas_column`, `expire_time_column`, `unique_idx_name_on_key`)
       VALUES ('listing_data', 'site', 'listing_cache', 'id', 'data', 
'0','version','0','PRIMARY');
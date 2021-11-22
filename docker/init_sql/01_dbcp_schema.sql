CREATE SCHEMA IF NOT EXISTS dbcp;

CREATE TABLE IF NOT EXISTS dbcp.test (
    id SERIAL PRIMARY KEY,
    valstr text,
    valint smallint
);

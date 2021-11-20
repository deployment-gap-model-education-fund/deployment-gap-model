-- NOTE: these sql files execute in lexicographical order.
-- Use numbered naming convention: XX_do-something.sql
CREATE USER IF NOT EXISTS dbcp_user WITH PASSWORD 'dbcp' CREATEDB;

CREATE DATABASE IF NOT EXISTS dbcp
    WITH
    OWNER = dbcp_user
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE EXTENSION IF NOT EXISTS postgis;

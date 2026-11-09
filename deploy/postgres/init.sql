-- One PostgreSQL server hosts every service's database locally, but each
-- service gets its own database and login (spec: "Data storage"). These
-- passwords are for local development only; the cluster uses Secrets.
CREATE ROLE accounts LOGIN PASSWORD 'accounts-dev';
CREATE DATABASE accounts OWNER accounts;
REVOKE ALL ON DATABASE accounts FROM PUBLIC;

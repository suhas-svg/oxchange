-- One PostgreSQL server hosts every service's database, but each service
-- gets its own database and login (spec: "Data storage").
--
-- Run by postgres-init.sh, which passes each login's password as a psql
-- variable: Compose uses fixed development passwords, and the Helm chart
-- takes them from a Secret. Used by both, so there is one copy.
CREATE ROLE accounts LOGIN PASSWORD :'accounts_password';
CREATE DATABASE accounts OWNER accounts;
REVOKE ALL ON DATABASE accounts FROM PUBLIC;

CREATE ROLE risk LOGIN PASSWORD :'risk_password';
CREATE DATABASE risk OWNER risk;
REVOKE ALL ON DATABASE risk FROM PUBLIC;

CREATE ROLE settlement LOGIN PASSWORD :'settlement_password';
CREATE DATABASE settlement OWNER settlement;
REVOKE ALL ON DATABASE settlement FROM PUBLIC;

CREATE ROLE market_data LOGIN PASSWORD :'market_data_password';
CREATE DATABASE market_data OWNER market_data;
REVOKE ALL ON DATABASE market_data FROM PUBLIC;

CREATE ROLE notifications LOGIN PASSWORD :'notifications_password';
CREATE DATABASE notifications OWNER notifications;
REVOKE ALL ON DATABASE notifications FROM PUBLIC;

-- The auditor reads every database through this read-only login (spec:
-- "Auditor"). Default privileges cover the tables each service's
-- migrations create later.
CREATE ROLE auditor LOGIN PASSWORD :'auditor_password';
ALTER ROLE auditor SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE accounts, risk, settlement TO auditor;
\connect accounts
GRANT USAGE ON SCHEMA public TO auditor;
ALTER DEFAULT PRIVILEGES FOR ROLE accounts IN SCHEMA public GRANT SELECT ON TABLES TO auditor;
\connect risk
GRANT USAGE ON SCHEMA public TO auditor;
ALTER DEFAULT PRIVILEGES FOR ROLE risk IN SCHEMA public GRANT SELECT ON TABLES TO auditor;
\connect settlement
GRANT USAGE ON SCHEMA public TO auditor;
ALTER DEFAULT PRIVILEGES FOR ROLE settlement IN SCHEMA public GRANT SELECT ON TABLES TO auditor;

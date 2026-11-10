-- One PostgreSQL server hosts every service's database locally, but each
-- service gets its own database and login (spec: "Data storage"). These
-- passwords are for local development only; the cluster uses Secrets.
CREATE ROLE accounts LOGIN PASSWORD 'accounts-dev';
CREATE DATABASE accounts OWNER accounts;
REVOKE ALL ON DATABASE accounts FROM PUBLIC;

CREATE ROLE risk LOGIN PASSWORD 'risk-dev';
CREATE DATABASE risk OWNER risk;
REVOKE ALL ON DATABASE risk FROM PUBLIC;

CREATE ROLE settlement LOGIN PASSWORD 'settlement-dev';
CREATE DATABASE settlement OWNER settlement;
REVOKE ALL ON DATABASE settlement FROM PUBLIC;

CREATE ROLE market_data LOGIN PASSWORD 'market-data-dev';
CREATE DATABASE market_data OWNER market_data;
REVOKE ALL ON DATABASE market_data FROM PUBLIC;

-- The auditor reads every database through this read-only login (spec:
-- "Auditor"). Default privileges cover the tables each service's
-- migrations create later.
CREATE ROLE auditor LOGIN PASSWORD 'auditor-dev';
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

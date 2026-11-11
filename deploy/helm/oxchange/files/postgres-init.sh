#!/bin/sh
# Creates the databases and logins on the server's first start. The
# postgres image runs this from /docker-entrypoint-initdb.d (it may source
# it, so no `set -u` here); the SQL sits outside that directory so it only
# runs through here, with the passwords filled in.
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname postgres   -v accounts_password="${ACCOUNTS_DB_PASSWORD:?}"   -v risk_password="${RISK_DB_PASSWORD:?}"   -v settlement_password="${SETTLEMENT_DB_PASSWORD:?}"   -v market_data_password="${MARKET_DATA_DB_PASSWORD:?}"   -v notifications_password="${NOTIFICATIONS_DB_PASSWORD:?}"   -v auditor_password="${AUDITOR_DB_PASSWORD:?}"   -f /init/postgres-init.sql

# Marketdata PostgreSQL Blueprint (Ansible)
Infrastructure-as-Code blueprint that provisions **PostgreSQL 15** and bootstraps a **marketdata** database for exchange feeds (candles / orderbooks / trades), using **daily range partitioning** and a hardened access model (**localhost-only + SSH tunneling**).

## What it solves
Provides a reproducible foundation for marketdata storage: baseline PostgreSQL tuning, opinionated roles for services (collector/analyst/partition_manager/reader), and safe partition lifecycle management (create + retention) via **SECURITY DEFINER** functions with allow-listing.

## What’s inside
### 1) PostgreSQL provisioning (`0_postgres_bootstrap`)
- Installs:
  - `postgresql-15`, `postgresql-client-15`
  - `python3-psycopg2` (required by `community.postgresql` Ansible modules)
- Renders `postgresql.conf` with baseline tuning:
  - memory (`shared_buffers`, `work_mem`, `maintenance_work_mem`)
  - connections (`max_connections`)
  - WAL/checkpoints
  - autovacuum (more aggressive than defaults)
  - timeouts (`statement_timeout`, `idle_in_transaction_session_timeout`)
  - planner / IO (`random_page_cost`, `effective_io_concurrency`)
  - parallelism (`max_worker_processes`, `max_parallel_workers`, `max_parallel_workers_per_gather`)
  - logging (collector, DDL logging, slow query threshold, lock waits)
  - locale set to `C`
- **Security-by-default**: `listen_addresses = 'localhost'` (no external bind)
- Creates the empty database: `marketdata`
### 2) Marketdata schema bootstrap (`1_marketdata_db_init`)
- Creates an exchange-specific schema: `{{ exchange }}` (default `gate`)
- Creates partitioned tables (RANGE by `timestamp`, daily partitions):
  - `{{ exchange }}.{{ currency }}_candles`
  - `{{ exchange }}.{{ currency }}_orderbooks`
  - `{{ exchange }}.{{ currency }}_trades`
- Creates a sequence for `trade_id`: `{{ exchange }}.{{ currency }}_trades_seq`
- Creates service roles (passwords provided via vault):
  - `collector` — write access
  - `analyst` — read access + TEMP
  - `partition_manager` — allowed to manage partitions via functions
  - `reader` — read-only
- Partition maintenance functions (**SECURITY DEFINER**, owner = `postgres`):
  - `{{ exchange }}.create_daily_partition(parent, day)`
  - `{{ exchange }}.drop_partitions_older_than(parent, before_day)`
- Function hardening:
  - `REVOKE ... FROM PUBLIC`
  - `GRANT EXECUTE ... TO partition_manager`
  - allow-list limited to the parent tables (candles/orderbooks/trades)

## Quick start
### Requirements
- Debian/Ubuntu on the target host (the role uses `apt`)
- Ansible on the control machine
- SSH access to the host(s) in the `db` inventory group
### 1) Install the Ansible collection
ansible-galaxy collection install -r requirements.yml
### 2) Run
ansible-playbook -i inventory/hosts.local.ini playbooks/main.yml

## Access model
PostgreSQL binds to localhost only (listen_addresses='localhost').
Remote access is expected to be done via SSH port-forwarding.
### SSH tunnel
ssh -i ~/.ssh/db -L 5432:127.0.0.1:5432 root@<ip-address>
### Connect with psql
psql -h 127.0.0.1 -p 5432 -U postgres -d marketdata

## Idempotency
The Ansible roles are idempotent (packages/config/service/database creation).
SQL bootstrap is safe to re-run: it relies on IF NOT EXISTS and CREATE OR REPLACE (for functions).
Re-running the playbook should not break an existing instance.

## Assumptions / limitations
pg_hba.conf is intentionally not managed here; access is expected via SSH tunneling.
Daily partitions are created and dropped via SECURITY DEFINER functions owned by postgres, callable by the partition_manager role.
# Marketdata K8s Prereqs (Ansible)

## Purpose
This playbook installs Kubernetes prerequisites for the **marketdata** namespace and services:
- Creates the `marketdata` **Namespace**
- Creates a `postgres` **Service** (ClusterIP, no selector) and matching **Endpoints** to route traffic to an **external PostgreSQL** instance
- Creates the `secret-marketdata-db` **Secret** (rendered from Ansible Vault) with DB credentials for project components

## Layout
```text
playbook.yml

roles/
  0_marketdata_namespace/
  1_marketdata_postgres_external/
  2_marketdata_secrets/

## Prerequisites

- Ansible connects to the target host via **SSH** (`hosts: master`)
- On the **target host**:
  - `kubectl` is installed and available in `$PATH`
  - Access to the Kubernetes API is configured (`kubeconfig` / active cluster context)
- The playbook runs with privilege escalation (`become: true`)
- For the secrets role: Ansible Vault password is available  
  (via `--vault-password-file` or interactive prompt)

## Execution Order

Roles are applied sequentially:

1. `0_marketdata_namespace`
2. `1_marketdata_postgres_external`  
   (Service is applied first, then Endpoints)
3. `2_marketdata_secrets`

## Run

```bash
ansible-playbook -i inventory/hosts.ini playbook.yml --vault-password-file vault_pass

## What the Playbook Validates

Each role:

- applies the manifest(s) using `kubectl apply`
- verifies resource existence with `kubectl get ...` using retries
- cleans up temporary manifest files from `/tmp`

## Result

After successful execution, the cluster contains:

- Namespace: `marketdata`
- External DB access inside the namespace via DNS name: `postgres`
- Secret: `secret-marketdata-db` with DB credentials for:
  - `collector`
  - `analyst`
  - `partition_manager`
  - `reader`

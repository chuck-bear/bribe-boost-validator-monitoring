# bboostd-validator-monitoring

A FastAPI application that queries SQLite databases in Kubernetes bboostd-validator pods and exposes execution metrics via Prometheus.

## Overview

This service monitors execution records from multiple bboostd validator pods (StatefulSets) by executing SQLite queries directly in each pod. It collects execution data including state, block ranges, timestamps, and errors, then exposes this information as Prometheus metrics and REST API endpoints.

## Features

- **Multi-pod monitoring**: Queries execution data from multiple validator pods in parallel
- **Prometheus metrics**: Exposes comprehensive metrics for all execution fields
- **REST API**: Provides JSON endpoints for querying execution data
- **Automatic refresh**: Periodically updates metrics every 60 seconds
- **Error handling**: Gracefully handles pod failures and query errors
- **Kubernetes integration**: Works both in-cluster and with kubeconfig
- **Caching**: Caches results for 5 minutes to reduce load

## Requirements

- Python 3.8+
- Kubernetes cluster with access to validator pods

### Python Dependencies

```bash
pip install fastapi uvicorn prometheus-client kubernetes pydantic
```

## Configuration

The service can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NAMESPACE` | `infra` | Kubernetes namespace where validator pods run |
| `POD_PREFIX` | `bboostd-validator-validator-` | Pod name prefix (before the number) |
| `POD_RANGE_START` | `0` | First pod number to query |
| `POD_RANGE_END` | `77` | Last pod number to query (inclusive) |
| `SQLITE_PATH` | `/opt/storage/database.db` | Path to SQLite database in pods |
| `QUERY` | `SELECT * FROM EXECUTIONS ORDER BY start_block DESC LIMIT 1` | SQLite query to execute |
| `QUERY_TIMEOUT` | `30` | Timeout in seconds for each query |
| `PORT` | `8080` | HTTP server port |
| `HOST` | `0.0.0.0` | HTTP server bind address |

### Pod Naming Pattern

The service expects StatefulSet pods with the naming pattern:
```
{POD_PREFIX}{NUMBER}-0
```

For example: `bboostd-validator-validator-0-0`, `bboostd-validator-validator-1-0`, etc.

## Usage

### Running Locally

```bash
python3 validator-executions-monitor.py
```

The service will:
1. Detect Kubernetes context (kubeconfig or in-cluster)
2. Detect namespace automatically
3. Start HTTP server on port 8080
4. Begin querying pods every 60 seconds

### Running in Kubernetes

TBD

### Service Account for In-Cluster

The service needs RBAC permissions to exec into pods:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: validator-executions-monitor
  namespace: infra
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: validator-executions-monitor
  namespace: infra
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: validator-executions-monitor
  namespace: infra
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: validator-executions-monitor
subjects:
- kind: ServiceAccount
  name: validator-executions-monitor
  namespace: infra
```

## API Endpoints

### GET `/`

Root endpoint with service information and available endpoints.

**Response:**
```json
{
  "service": "Bboostd Validator Executions Monitor",
  "version": "1.0.0",
  "namespace": "infra",
  "pod_range": "bboostd-validator-validator-0-77 (pods: ...)",
  "endpoints": { ... }
}
```

### GET `/executions`

Get execution information for all validator pods.

**Query Parameters:**
- `force_refresh` (boolean, default: false): Force refresh instead of using cache

**Response:** `ClusterExecutionSummary` with list of all pod executions

### GET `/executions/summary`

Get execution summary (same as `/executions`).

### GET `/pod/{pod_name}`

Get execution information for a specific pod.

**Example:**
```bash
curl http://localhost:8080/pod/bboostd-validator-validator-0-0
```

### GET `/metrics`

Prometheus metrics endpoint. Exposes all execution metrics in Prometheus format.

**Example:**
```bash
curl http://localhost:8080/metrics
```

### GET `/health`

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00",
  "kubernetes_connected": true,
  "namespace": "infra",
  "last_scan": "2024-01-01T12:00:00"
}
```

### POST `/executions/refresh`

Force immediate refresh of all execution data.

## Prometheus Metrics

### Execution Metrics

All metrics are labeled with `pod_name` and `namespace`.

| Metric | Type | Description |
|--------|------|-------------|
| `bboostd_validator_execution_state` | Gauge | Execution state (0=pending, 1=running, 2=completed, 3=failed) with `state` label |
| `bboostd_validator_execution_state_numeric` | Gauge | Execution state as numeric (same values, no state label) |
| `bboostd_validator_execution_start_block` | Gauge | Start block number (-1 if missing) |
| `bboostd_validator_execution_end_block` | Gauge | End block number (-1 if missing) |
| `bboostd_validator_execution_start_timestamp` | Gauge | Start timestamp (Unix time, 0 if missing) |
| `bboostd_validator_execution_end_timestamp` | Gauge | End timestamp (Unix time, 0 if missing) |
| `bboostd_validator_execution_has_error` | Gauge | Error flag (1=error, 0=no error) |
| `bboostd_validator_execution_error` | Gauge | Error text hash (0 if no error) |
| `bboostd_validator_execution_blocks_processed` | Gauge | Blocks processed (end_block - start_block, -1 if missing) |
| `bboostd_validator_execution_duration_seconds` | Gauge | Execution duration in seconds (end_timestamp - start_timestamp, -1 if missing) |

### Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `bboostd_validator_query_duration_seconds` | Histogram | Time spent querying each pod |
| `bboostd_validator_query_errors_total` | Counter | Total number of query errors per pod |
| `api_requests_total` | Counter | Total API requests by endpoint and method |

## Example Queries

### Prometheus Query Examples

**Count pods by execution state:**
```promql
count by (state) (bboostd_validator_execution_state)
```

**Find pods with errors:**
```promql
bboostd_validator_execution_has_error == 1
```

**Average execution duration:**
```promql
avg(bboostd_validator_execution_duration_seconds)
```

**Pods with longest execution duration:**
```promql
topk(10, bboostd_validator_execution_duration_seconds)
```

**Sum of blocks processed across all pods:**
```promql
sum(bboostd_validator_execution_blocks_processed)
```

**Query error rate:**
```promql
rate(bboostd_validator_query_errors_total[5m])
```

### REST API Examples

**Get all executions:**
```bash
curl http://localhost:8080/executions
```

**Force refresh and get summary:**
```bash
curl "http://localhost:8080/executions/summary?force_refresh=true"
```

**Get specific pod execution:**
```bash
curl http://localhost:8080/pod/bboostd-validator-validator-42-0
```

## Database Schema

The service expects an `EXECUTIONS` table with the following schema:

```sql
CREATE TABLE EXECUTIONS (
    id TEXT PRIMARY KEY,
    state TEXT,
    start_block INTEGER,
    end_block INTEGER,
    start_timestamp INTEGER,
    end_timestamp INTEGER,
    error TEXT
);
```

The query executed is:
```sql
SELECT * FROM EXECUTIONS ORDER BY start_block DESC LIMIT 1
```

This retrieves the most recent execution (by start_block).

## Monitoring and Alerting

### Recommended Prometheus Alerts

**High error rate:**
```yaml
alert: ValidatorExecutionHighErrorRate
expr: rate(bboostd_validator_query_errors_total[5m]) > 0.1
for: 5m
```

**Pods with execution errors:**
```yaml
alert: ValidatorExecutionHasErrors
expr: bboostd_validator_execution_has_error == 1
for: 1m
```

**Failed executions:**
```yaml
alert: ValidatorExecutionFailed
expr: bboostd_validator_execution_state_numeric == 3
for: 5m
```

**Slow queries:**
```yaml
alert: ValidatorQuerySlow
expr: bboostd_validator_query_duration_seconds > 10
for: 5m
```

## Troubleshooting

### All pods failing (404 errors)

- Verify pod names match the expected pattern: `{prefix}{number}-0`
- Check that pods exist: `kubectl get pods -n infra | grep validator`
- Verify namespace is correct

### Query errors

- Ensure sqlite3 is installed in validator pods
- Verify database path is correct (`/opt/storage/database.db`)
- Check database permissions
- Verify EXECUTIONS table exists with expected schema

### Permission errors

- Verify service account has `pods/exec` permissions
- Check RBAC role bindings
- Ensure running in correct namespace

### Metrics not updating

- Check `/health` endpoint for connection status
- Verify periodic refresh is running (every 60 seconds)
- Check logs for errors
- Use `/executions/refresh` to force manual refresh

## Logging

The service uses the logger name `bboostd-validator-monitoring`. Logs include:
- Kubernetes connection status
- Pod query results
- Error details
- Scan completion summaries

Log level is INFO by default. Set `LOG_LEVEL` environment variable to change:
- `DEBUG`: Detailed debug information
- `INFO`: Standard operational messages
- `WARNING`: Warning messages only
- `ERROR`: Error messages only

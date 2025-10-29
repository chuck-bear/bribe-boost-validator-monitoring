#!/usr/bin/env python3
"""
Bboostd Validator Executions Monitor
A FastAPI application that queries SQLite databases in validator pods
and exposes metrics via Prometheus
"""

import logging
import json
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from contextlib import asynccontextmanager
import os
import asyncio
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
)
from prometheus_client.core import CollectorRegistry
from starlette.responses import Response
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('bboostd-validator-monitoring')

# Prometheus metrics
registry = CollectorRegistry()

# Execution metrics - one gauge per column
execution_state_gauge = Gauge(
    'bboostd_validator_execution_state',
    ('Latest execution state from validator pod '
     '(0=unknown, 1=running, 2=completed, 3=failed)'),
    ['pod_name', 'namespace', 'state'], registry=registry
)

execution_state_numeric_gauge = Gauge(
    'bboostd_validator_execution_state_numeric',
    ('Latest execution state as numeric value '
     '(0=unknown/pending, 1=running, 2=completed, 3=failed)'),
    ['pod_name', 'namespace'], registry=registry
)

execution_start_block_gauge = Gauge(
    'bboostd_validator_execution_start_block',
    'Latest execution start block number',
    ['pod_name', 'namespace'], registry=registry
)

execution_end_block_gauge = Gauge(
    'bboostd_validator_execution_end_block',
    'Latest execution end block number',
    ['pod_name', 'namespace'], registry=registry
)

execution_start_timestamp_gauge = Gauge(
    'bboostd_validator_execution_start_timestamp',
    'Latest execution start timestamp (Unix timestamp)',
    ['pod_name', 'namespace'], registry=registry
)

execution_end_timestamp_gauge = Gauge(
    'bboostd_validator_execution_end_timestamp',
    'Latest execution end timestamp (Unix timestamp)',
    ['pod_name', 'namespace'], registry=registry
)

execution_has_error_gauge = Gauge(
    'bboostd_validator_execution_has_error',
    'Whether the latest execution has an error (1=error, 0=no error)',
    ['pod_name', 'namespace'], registry=registry
)

execution_error_text_gauge = Gauge(
    'bboostd_validator_execution_error',
    'Execution error text (as hash value, 0 if no error)',
    ['pod_name', 'namespace'], registry=registry
)

execution_blocks_processed_gauge = Gauge(
    'bboostd_validator_execution_blocks_processed',
    'Number of blocks processed in latest execution (end_block - start_block)',
    ['pod_name', 'namespace'], registry=registry
)

execution_duration_seconds_gauge = Gauge(
    'bboostd_validator_execution_duration_seconds',
    ('Duration of latest execution in seconds '
     '(end_timestamp - start_timestamp)'),
    ['pod_name', 'namespace'], registry=registry
)

query_duration_histogram = Histogram(
    'bboostd_validator_query_duration_seconds',
    'Time spent querying validator pods',
    ['pod_name', 'namespace'], registry=registry
)

query_errors_counter = Counter(
    'bboostd_validator_query_errors_total',
    'Total number of query errors',
    ['pod_name', 'namespace'], registry=registry
)

api_requests_counter = Counter(
    'api_requests_total', 'Total API requests',
    ['endpoint', 'method'], registry=registry
)

# Pydantic models


class ExecutionRecord(BaseModel):
    id: str
    state: str
    start_block: Optional[int] = None
    end_block: Optional[int] = None
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None
    error: Optional[str] = None


class PodExecutionInfo(BaseModel):
    pod_name: str
    namespace: str
    execution: Optional[ExecutionRecord] = None
    query_success: bool
    error_message: Optional[str] = None
    last_queried: datetime


class ClusterExecutionSummary(BaseModel):
    total_pods: int
    successful_queries: int
    failed_queries: int
    pod_executions: List[PodExecutionInfo]
    last_updated: datetime


@dataclass
class QueryConfig:
    namespace: str
    pod_prefix: str = "bboostd-validator-validator-"
    pod_range_start: int = 0
    pod_range_end: int = 77
    sqlite_path: str = "/opt/storage/database.db"
    query: str = "SELECT * FROM EXECUTIONS ORDER BY start_block DESC LIMIT 1"
    query_timeout: int = 30


class KubernetesExecutor:
    """Service to execute commands in Kubernetes pods"""

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)
        try:
            # Try to load in-cluster config first
            try:
                config.load_incluster_config()
                logger.info("Loaded in-cluster Kubernetes config")
            except config.ConfigException:
                config.load_kube_config()
                logger.info("Loaded Kubernetes config from kubeconfig")

            self.v1 = client.CoreV1Api()
            self.current_namespace = self._detect_namespace()
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            self.v1 = None
            self.current_namespace = os.getenv('NAMESPACE', 'infra')

    def _detect_namespace(self) -> str:
        """Detect the current namespace"""
        # Try to read from service account (in-cluster)
        try:
            ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
            with open(ns_path, 'r') as f:
                namespace = f.read().strip()
                logger.info(f"Detected namespace: {namespace}")
                return namespace
        except Exception:
            pass

        # Fallback to environment variable or default
        namespace = os.getenv('NAMESPACE', 'infra')
        logger.info(f"Using namespace: {namespace}")
        return namespace

    def _exec_command_sync(self, pod_name: str, namespace: str,
                           command: List[str],
                           timeout: int = 30) -> Tuple[bool, str, str]:
        """Execute a command in a pod synchronously"""
        if not self.v1:
            return False, "", "Kubernetes client not initialized"

        try:
            # Execute command in pod
            resp = stream(
                self.v1.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _request_timeout=timeout
            )

            # resp can contain both stdout and stderr mixed
            output = resp if resp else ""

            # Try to split stdout and stderr if possible
            # In practice, stream returns combined output
            stdout = output
            stderr = ""

            return True, stdout, stderr

        except ApiException as e:
            error_msg = f"API error: {e.reason}"
            logger.error(f"Failed to exec in pod {pod_name}: {error_msg}")
            return False, "", error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Failed to exec in pod {pod_name}: {error_msg}")
            return False, "", error_msg

    async def exec_command_in_pod(self, pod_name: str, namespace: str,
                                  command: List[str],
                                  timeout: int = 30) -> Tuple[bool, str, str]:
        """Execute a command in a pod and return (success, stdout, stderr)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._exec_command_sync,
            pod_name,
            namespace,
            command,
            timeout
        )

    async def query_sqlite(
            self, pod_name: str, namespace: str,
            sqlite_path: str, query: str,
            timeout: int = 30) -> Tuple[
                bool, Optional[ExecutionRecord], Optional[str]]:
        """Query SQLite database in a pod and parse the result"""
        # Match the original kubectl command format exactly
        # Format: sqlite3 /opt/storage/database.db "SELECT ..."
        # Pass sqlite3, db path, and query as separate arguments
        command = ["sqlite3", sqlite_path, query]

        labels = {'pod_name': pod_name, 'namespace': namespace}
        with query_duration_histogram.labels(**labels).time():
            success, stdout, stderr = await self.exec_command_in_pod(
                pod_name, namespace, command, timeout
            )

            if not success:
                query_errors_counter.labels(**labels).inc()
                error_detail = stderr or stdout or "Unknown error"
                logger.debug(
                    f"Query failed for {pod_name}: {error_detail[:500]}")
                return False, None, error_detail

            # Parse output - sqlite3 returns pipe-delimited by default
            try:
                output = stdout.strip()
                if not output:
                    return False, None, "Empty response from pod"

                # Try to parse as JSON first (in case -json was added)
                try:
                    data = json.loads(output)
                    if isinstance(data, list):
                        if len(data) == 0:
                            return False, None, "No records found"
                        data = data[0]
                    if not isinstance(data, dict):
                        raise ValueError("JSON is not an object")
                except (json.JSONDecodeError, ValueError):
                    # Not JSON, parse as pipe-delimited (sqlite3 default)
                    # Expected format: id|state|start_block|end_block|...|error
                    parts = output.split('|')
                    if len(parts) != 7:
                        msg = (
                            f"Unexpected column count: expected 7, got "
                            f"{len(parts)}. Output: {output[:200]}")
                        return False, None, msg

                    data = {
                        'id': parts[0] if parts[0] else None,
                        'state': parts[1] if parts[1] else None,
                        'start_block': int(parts[2]) if parts[2] else None,
                        'end_block': int(parts[3]) if parts[3] else None,
                        'start_timestamp': (
                            int(parts[4]) if parts[4] else None),
                        'end_timestamp': (
                            int(parts[5]) if parts[5] else None),
                        'error': parts[6] if parts[6] else None
                    }

                # Map the fields to ExecutionRecord
                def safe_int(value):
                    return int(value) if value else None

                def safe_str(value):
                    return str(value) if value else ''

                execution = ExecutionRecord(
                    id=safe_str(data.get('id')),
                    state=safe_str(data.get('state')),
                    start_block=safe_int(data.get('start_block')),
                    end_block=safe_int(data.get('end_block')),
                    start_timestamp=safe_int(data.get('start_timestamp')),
                    end_timestamp=safe_int(data.get('end_timestamp')),
                    error=safe_str(data.get('error'))
                )

                return True, execution, None

            except (ValueError, IndexError) as e:
                query_errors_counter.labels(**labels).inc()
                msg = (
                    f"Failed to parse output: {str(e)}, "
                    f"output: {output[:200]}")
                return False, None, msg
            except Exception as e:
                query_errors_counter.labels(**labels).inc()
                msg = f"Unexpected error parsing response: {str(e)}"
                return False, None, msg


class ValidatorExecutionMonitor:
    """Main service for monitoring validator executions"""

    def __init__(self, query_config: QueryConfig):
        self.k8s_executor = KubernetesExecutor()
        self.query_config = query_config
        self.last_scan = None
        self.cached_results = None
        self.cache_duration = timedelta(minutes=5)

    async def scan_all_validators(
            self, force_refresh: bool = False) -> ClusterExecutionSummary:
        """Query all validator pods and collect execution data"""

        # Check cache
        if (not force_refresh and self.cached_results and
                self.last_scan and
                datetime.now() - self.last_scan < self.cache_duration):
            return self.cached_results

        namespace = self.query_config.namespace
        pod_prefix = self.query_config.pod_prefix
        pod_range_start = self.query_config.pod_range_start
        pod_range_end = self.query_config.pod_range_end
        sqlite_path = self.query_config.sqlite_path
        query = self.query_config.query

        pod_executions = []
        successful_queries = 0
        failed_queries = 0

        # Query all pods in parallel
        tasks = []
        for pod_num in range(pod_range_start, pod_range_end + 1):
            # StatefulSet pods follow pattern: {name}-{ordinal}
            # Since replicas=1, ordinal is always 0
            pod_name = f"{pod_prefix}{pod_num}-0"
            task = self._query_single_pod(
                pod_name, namespace, sqlite_path, query)
            tasks.append((pod_name, task))

        # Wait for all queries with timeout
        task_list = [task for _, task in tasks]
        results = await asyncio.gather(*task_list, return_exceptions=True)

        for (pod_name, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                error_msg = str(result)
                logger.error(f"Exception querying pod {pod_name}: {error_msg}")
                pod_executions.append(PodExecutionInfo(
                    pod_name=pod_name,
                    namespace=namespace,
                    execution=None,
                    query_success=False,
                    error_message=error_msg,
                    last_queried=datetime.now()
                ))
                failed_queries += 1
                labels = {'pod_name': pod_name, 'namespace': namespace}
                query_errors_counter.labels(**labels).inc()
                continue

            success, execution, error_msg = result
            if success and execution:
                successful_queries += 1
                # Update Prometheus metrics
                self._update_prometheus_metrics(pod_name, namespace, execution)
            else:
                failed_queries += 1
                # Log detailed error for first few failures to help debug
                if failed_queries <= 3:
                    logger.warning(
                        f"Query failed for pod {pod_name}: {error_msg}")

            pod_executions.append(PodExecutionInfo(
                pod_name=pod_name,
                namespace=namespace,
                execution=execution if success else None,
                query_success=success,
                error_message=error_msg,
                last_queried=datetime.now()
            ))

        summary = ClusterExecutionSummary(
            total_pods=len(pod_executions),
            successful_queries=successful_queries,
            failed_queries=failed_queries,
            pod_executions=pod_executions,
            last_updated=datetime.now()
        )

        # Cache results
        self.cached_results = summary
        self.last_scan = datetime.now()

        msg = (f"Scan completed: {successful_queries} successful, "
               f"{failed_queries} failed out of {len(pod_executions)} pods")
        logger.info(msg)

        return summary

    async def _query_single_pod(
            self, pod_name: str, namespace: str,
            sqlite_path: str, query: str) -> Tuple[
                bool, Optional[ExecutionRecord], Optional[str]]:
        """Query a single pod"""
        return await self.k8s_executor.query_sqlite(
            pod_name, namespace, sqlite_path, query,
            self.query_config.query_timeout)

    def _update_prometheus_metrics(
            self, pod_name: str, namespace: str,
            execution: ExecutionRecord):
        """Update Prometheus metrics for a pod's execution"""
        labels = {'pod_name': pod_name, 'namespace': namespace}

        # State - map to numeric value and use as both value and label
        state_mapping = {
            'running': 1,
            'completed': 2,
            'failed': 3,
            'pending': 0
        }
        state_value = state_mapping.get(execution.state.lower(), 0)

        # Set state with label (for filtering)
        execution_state_gauge.labels(
            state=execution.state, **labels
        ).set(state_value)

        # Set numeric state without label (for simpler queries)
        execution_state_numeric_gauge.labels(**labels).set(state_value)

        # Start block (set to -1 if None to indicate missing)
        start_block = (
            execution.start_block if execution.start_block is not None else -1
        )
        execution_start_block_gauge.labels(**labels).set(start_block)

        # End block (set to -1 if None to indicate missing)
        end_block = (
            execution.end_block if execution.end_block is not None else -1
        )
        execution_end_block_gauge.labels(**labels).set(end_block)

        # Start timestamp (set to 0 if None)
        start_ts = (
            execution.start_timestamp
            if execution.start_timestamp is not None else 0
        )
        execution_start_timestamp_gauge.labels(**labels).set(start_ts)

        # End timestamp (set to 0 if None)
        end_ts = (
            execution.end_timestamp
            if execution.end_timestamp is not None else 0
        )
        execution_end_timestamp_gauge.labels(**labels).set(end_ts)

        # Error handling
        has_error = 1 if execution.error else 0
        execution_has_error_gauge.labels(**labels).set(has_error)

        # Error text as hash (for tracking specific errors, 0 if no error)
        error_hash = hash(execution.error) if execution.error else 0
        execution_error_text_gauge.labels(**labels).set(error_hash)

        # Derived metrics: blocks processed
        if (execution.start_block is not None and
                execution.end_block is not None):
            blocks_processed = execution.end_block - execution.start_block
            execution_blocks_processed_gauge.labels(**labels).set(
                blocks_processed
            )
        else:
            execution_blocks_processed_gauge.labels(**labels).set(-1)

        # Derived metrics: execution duration in seconds
        if (execution.start_timestamp is not None and
                execution.end_timestamp is not None):
            duration = execution.end_timestamp - execution.start_timestamp
            execution_duration_seconds_gauge.labels(**labels).set(duration)
        else:
            execution_duration_seconds_gauge.labels(**labels).set(-1)


# Initialize configuration
query_config = QueryConfig(
    namespace=os.getenv('NAMESPACE', 'infra'),
    pod_prefix=os.getenv('POD_PREFIX', 'bboostd-validator-validator-'),
    pod_range_start=int(os.getenv('POD_RANGE_START', '0')),
    pod_range_end=int(os.getenv('POD_RANGE_END', '77')),
    sqlite_path=os.getenv('SQLITE_PATH', '/opt/storage/database.db'),
    query=os.getenv(
        'QUERY',
        'SELECT * FROM EXECUTIONS ORDER BY start_block DESC LIMIT 1'
    ),
    query_timeout=int(os.getenv('QUERY_TIMEOUT', '30'))
)

# Initialize monitor
monitor = ValidatorExecutionMonitor(query_config)


# Background task to periodically refresh metrics
async def periodic_refresh():
    """Periodically refresh metrics"""
    while True:
        try:
            await asyncio.sleep(60)  # Refresh every minute
            await monitor.scan_all_validators(force_refresh=False)
        except Exception as e:
            logger.error(f"Error in periodic refresh: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    refresh_task = asyncio.create_task(periodic_refresh())
    # Initial scan
    asyncio.create_task(
        monitor.scan_all_validators(force_refresh=True))
    yield
    # Shutdown
    refresh_task.cancel()
    try:
        await refresh_task
    except asyncio.CancelledError:
        pass


# FastAPI app
app = FastAPI(
    title="Bboostd Validator Executions Monitor",
    description=(
        "Monitor SQLite execution records from validator pods "
        "and expose via Prometheus"
    ),
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Middleware to track API requests"""
    api_requests_counter.labels(
        endpoint=request.url.path,
        method=request.method
    ).inc()

    response = await call_next(request)
    return response


@app.get("/")
async def root():
    return {
        "service": "Bboostd Validator Executions Monitor",
        "version": "1.0.0",
        "namespace": query_config.namespace,
        "pod_range": (
            f"{query_config.pod_prefix}{query_config.pod_range_start}-"
            f"{query_config.pod_range_end} (pods: "
            f"{query_config.pod_prefix}{query_config.pod_range_start}-0 to "
            f"{query_config.pod_prefix}{query_config.pod_range_end}-0)"),
        "endpoints": {
            "executions": "/executions - Get all pod execution data",
            "executions/summary": "/executions/summary - Get summary",
            "pod/{pod_name}": "/pod/{pod_name} - Get specific pod data",
            "metrics": "/metrics - Prometheus metrics",
            "health": "/health - Health check"
        }
    }


@app.get("/executions", response_model=ClusterExecutionSummary)
async def get_all_executions(force_refresh: bool = False):
    """Get execution information for all validator pods"""
    try:
        return await monitor.scan_all_validators(force_refresh)
    except Exception as e:
        logger.error(f"Error scanning validator pods: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/executions/summary", response_model=ClusterExecutionSummary)
async def get_execution_summary(force_refresh: bool = False):
    """Get execution summary for all pods"""
    try:
        return await monitor.scan_all_validators(force_refresh)
    except Exception as e:
        logger.error(f"Error getting execution summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pod/{pod_name}", response_model=PodExecutionInfo)
async def get_pod_execution(pod_name: str):
    """Get execution information for a specific pod"""
    try:
        summary = await monitor.scan_all_validators()

        for pod_info in summary.pod_executions:
            if pod_info.pod_name == pod_name:
                return pod_info

        raise HTTPException(
            status_code=404, detail=f"Pod {pod_name} not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pod execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    # Trigger a background scan to keep metrics fresh
    asyncio.create_task(
        monitor.scan_all_validators(force_refresh=False))
    return Response(
        generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "kubernetes_connected": monitor.k8s_executor.v1 is not None,
        "namespace": query_config.namespace,
        "last_scan": monitor.last_scan
    }


@app.post("/executions/refresh")
async def refresh_executions():
    """Force refresh of execution data"""
    try:
        summary = await monitor.scan_all_validators(force_refresh=True)
        return {
            "message": "Execution data refresh completed",
            "total_pods": summary.total_pods,
            "successful_queries": summary.successful_queries,
            "failed_queries": summary.failed_queries
        }
    except Exception as e:
        logger.error(f"Error refreshing executions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOST", "0.0.0.0")

    logger.info(
        f"Starting Bboostd Validator Executions Monitor on {host}:{port}")
    pod_range_msg = (
        f"Monitoring pods: {query_config.pod_prefix}"
        f"{query_config.pod_range_start}-0 to "
        f"{query_config.pod_prefix}{query_config.pod_range_end}-0 "
        f"in namespace {query_config.namespace}")
    logger.info(pod_range_msg)
    uvicorn.run(app, host=host, port=port)

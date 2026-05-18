import asyncio
import json
import logging
import os
from enum import Enum
from urllib.parse import quote

import aiohttp
from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.metrics.metrics_head import (
    DEFAULT_PROMETHEUS_HEADERS,
    DEFAULT_PROMETHEUS_HOST,
    PROMETHEUS_HEADERS_ENV_VAR,
    PROMETHEUS_HOST_ENV_VAR,
    PrometheusQueryError,
    parse_prom_headers,
)
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Window and sampling rate used for certain Prometheus queries.
# Datapoints up until `MAX_TIME_WINDOW` ago are queried at `SAMPLE_RATE` intervals.
MAX_TIME_WINDOW = "1h"
SAMPLE_RATE = "1s"


class PrometheusQuery(Enum):
    """Enum to store types of Prometheus queries for a given metric and grouping."""

    VALUE = ("value", "sum({}{{SessionName='{}'}}) by ({})")
    MAX = (
        "max",
        "max_over_time(sum({}{{SessionName='{}'}}) by ({})["
        + f"{MAX_TIME_WINDOW}:{SAMPLE_RATE}])",
    )


DATASET_METRICS = {
    "ray_data_output_rows": (PrometheusQuery.MAX,),
    "ray_data_spilled_bytes": (PrometheusQuery.MAX,),
    "ray_data_current_bytes": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
    "ray_data_cpu_usage_cores": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
    "ray_data_gpu_usage_cores": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
}

class DataHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        self.prometheus_headers = parse_prom_headers(
            os.environ.get(
                PROMETHEUS_HEADERS_ENV_VAR,
                DEFAULT_PROMETHEUS_HEADERS,
            )
        )
        self._execution_config_client = None

    @staticmethod
    def _json_response(data: dict, status: int = 200) -> Response:
        """Build a JSON response with the given data and status code."""
        return Response(
            text=json.dumps(data),
            content_type="application/json",
            status=status,
        )

    def _get_execution_config_client(self):
        """Lazily initialize the ExecutionConfigStorageClient."""
        if self._execution_config_client is None:
            from ray.dashboard.modules.data.config_client import (
                ExecutionConfigStorageClient,
            )
            self._execution_config_client = ExecutionConfigStorageClient(self.gcs_client)
        return self._execution_config_client

    async def _parse_config_from_request(self, req: Request):
        """Parse and validate ExecutionConfig from a request body.

        Returns:
            Tuple of (config, None) on success, or (None, error_response) on failure.
        """
        try:
            body = await req.json()
        except json.JSONDecodeError as e:
            return None, self._json_response(
                {"error": f"Invalid JSON in request body: {e}"}, status=400
            )

        config_data = body.get("config")
        if config_data is None:
            return None, self._json_response(
                {"error": "Missing 'config' field in request body"}, status=400
            )

        try:
            from ray.data._internal.execution.config import ExecutionConfig
            config = ExecutionConfig.from_dict(config_data)
        except (ValueError, KeyError) as e:
            return None, self._json_response(
                {"error": f"Invalid config format: {e}"}, status=400
            )

        return config, None

    @routes.get("/api/data/datasets/{job_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_datasets(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]

        try:
            from ray.data._internal.stats import get_or_create_stats_actor

            _stats_actor = get_or_create_stats_actor()
            datasets = await _stats_actor.get_datasets.remote(job_id)
            # Initializes dataset metric values
            for dataset in datasets:
                for metric, queries in DATASET_METRICS.items():
                    datasets[dataset][metric] = {query.value[0]: 0 for query in queries}
                    for operator in datasets[dataset]["operators"]:
                        datasets[dataset]["operators"][operator][metric] = {
                            query.value[0]: 0 for query in queries
                        }
            # Query dataset metric values from prometheus
            try:
                # TODO (Zandew): store results of completed datasets in stats actor.
                for metric, queries in DATASET_METRICS.items():
                    for query in queries:
                        query_name, prom_query = query.value
                        # Dataset level
                        dataset_result = await self._query_prometheus(
                            prom_query.format(metric, self.session_name, "dataset")
                        )
                        for res in dataset_result["data"]["result"]:
                            dataset, value = res["metric"]["dataset"], res["value"][1]
                            if dataset in datasets:
                                datasets[dataset][metric][query_name] = value

                        # Operator level
                        operator_result = await self._query_prometheus(
                            prom_query.format(
                                metric, self.session_name, "dataset, operator"
                            )
                        )
                        for res in operator_result["data"]["result"]:
                            dataset, operator, value = (
                                res["metric"]["dataset"],
                                res["metric"]["operator"],
                                res["value"][1],
                            )
                            # Check if dataset/operator is in current _StatsActor scope.
                            # Prometheus server may contain metrics from previous
                            # cluster if not reset.
                            if (
                                dataset in datasets
                                and operator in datasets[dataset]["operators"]
                            ):
                                datasets[dataset]["operators"][operator][metric][
                                    query_name
                                ] = value
            except (
                aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ConnectionTimeoutError,
                asyncio.TimeoutError,
            ):
                # Prometheus server may not be running or not reachable,
                # leave these values blank and return other data
                logging.exception(
                    "Exception occurred while querying Prometheus. "
                    "The Prometheus server may not be running."
                )
            # Flatten response
            for dataset in datasets:
                datasets[dataset]["operators"] = list(
                    map(
                        lambda item: {"operator": item[0], **item[1]},
                        datasets[dataset]["operators"].items(),
                    )
                )
            datasets = list(
                map(lambda item: {"dataset": item[0], **item[1]}, datasets.items())
            )
            # Sort by descending start time
            datasets = sorted(datasets, key=lambda x: x["start_time"], reverse=True)
            return Response(
                text=json.dumps({"datasets": datasets}),
                content_type="application/json",
            )
        except Exception as e:
            logging.exception("Exception occurred while getting datasets.")
            return Response(
                status=503,
                text=str(e),
            )

    async def _query_prometheus(self, query):
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}",
            headers=self.prometheus_headers,
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                return prom_data

            message = await resp.text()
            raise PrometheusQueryError(resp.status, message)

    @routes.get("/api/data/{job_id}/execution_config")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_execution_config(self, req: Request) -> Response:
        """Get all dataset-level execution configurations for a specific job."""
        job_id = req.match_info["job_id"]

        try:
            client = self._get_execution_config_client()
            dataset_configs = await client.get_configs_for_job(job_id)

            if not dataset_configs:
                return self._json_response(
                    {"error": f"Execution config not found for job: {job_id}"},
                    status=404,
                )

            result = {
                dataset_id: config.to_dict()
                for dataset_id, config in dataset_configs.items()
            }
            return self._json_response({"job_id": job_id, "datasets": result})
        except Exception as e:
            logger.exception(f"Failed to get execution config for job {job_id}")
            return self._json_response({"error": str(e)}, status=500)

    @routes.get("/api/data/{job_id}/{dataset_id}/execution_config")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_dataset_execution_config(self, req: Request) -> Response:
        """Get the execution configuration for a specific dataset within a job."""
        job_id = req.match_info["job_id"]
        dataset_id = req.match_info["dataset_id"]

        try:
            client = self._get_execution_config_client()
            config = await client.get_config_for_dataset(job_id, dataset_id)

            if config is None:
                return self._json_response(
                    {"error": f"Execution config not found for job: {job_id}, dataset: {dataset_id}"},
                    status=404,
                )

            return self._json_response({
                "job_id": job_id,
                "dataset_id": dataset_id,
                "config": config.to_dict(),
            })
        except Exception as e:
            logger.exception(
                f"Failed to get execution config for job {job_id}, dataset {dataset_id}"
            )
            return self._json_response({"error": str(e)}, status=500)

    @routes.put("/api/data/{job_id}/{dataset_id}/execution_config")
    @optional_utils.init_ray_and_catch_exceptions()
    async def put_dataset_execution_config(self, req: Request) -> Response:
        """Set or update the execution configuration for a specific dataset within a job."""
        job_id = req.match_info["job_id"]
        dataset_id = req.match_info["dataset_id"]

        try:
            config, error_response = await self._parse_config_from_request(req)
            if error_response is not None:
                return error_response

            client = self._get_execution_config_client()
            is_new = await client.put_config_for_dataset(job_id, dataset_id, config)

            return self._json_response(
                {
                    "success": True,
                    "job_id": job_id,
                    "dataset_id": dataset_id,
                    "created": is_new,
                    "config": config.to_dict(),
                },
                status=201 if is_new else 200,
            )
        except Exception as e:
            logger.exception(
                f"Failed to put execution config for job {job_id}, dataset {dataset_id}"
            )
            return self._json_response({"error": str(e)}, status=500)

    @routes.delete("/api/data/{job_id}/{dataset_id}/execution_config")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_dataset_execution_config(self, req: Request) -> Response:
        """Delete the execution configuration for a specific dataset within a job."""
        job_id = req.match_info["job_id"]
        dataset_id = req.match_info["dataset_id"]

        try:
            client = self._get_execution_config_client()
            deleted = await client.delete_config_for_dataset(job_id, dataset_id)

            if not deleted:
                return self._json_response(
                    {"error": f"Execution config not found for job: {job_id}, dataset: {dataset_id}"},
                    status=404,
                )

            return self._json_response({
                "success": True,
                "job_id": job_id,
                "dataset_id": dataset_id,
                "deleted": True,
            })
        except Exception as e:
            logger.exception(
                f"Failed to delete execution config for job {job_id}, dataset {dataset_id}"
            )
            return self._json_response({"error": str(e)}, status=500)

    @routes.get("/api/data/execution_configs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_execution_configs(self, req: Request) -> Response:
        """List all execution configurations grouped by job_id."""
        try:
            client = self._get_execution_config_client()
            configs = await client.get_all_configs()

            result = {
                job_id: {
                    dataset_id: config.to_dict()
                    for dataset_id, config in dataset_configs.items()
                }
                for job_id, dataset_configs in configs.items()
            }

            return self._json_response({"configs": result})
        except Exception as e:
            logger.exception("Failed to list execution configs")
            return self._json_response({"error": str(e)}, status=500)

    @routes.get("/api/data/{job_id}/operators")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_operators(self, req: Request) -> Response:
        """Get operator runtime metrics for a specific job.

        Returns operator metrics including TaskPool and ActorPool specific information.
        """
        job_id = req.match_info["job_id"]

        try:
            from ray.data._internal.stats import get_or_create_stats_actor

            _stats_actor = get_or_create_stats_actor()
            datasets = await _stats_actor.get_datasets.remote(job_id)

            if not datasets:
                return self._json_response(
                    {"error": f"No datasets found for job: {job_id}", "job_id": job_id},
                    status=404,
                )

            # Extract operator information with pool-specific metrics
            result = []
            for dataset_tag, dataset_info in datasets.items():
                operators_info = dataset_info.get("operators", {})

                for op_id, op_data in operators_info.items():
                    operator_info = {
                        "dataset": dataset_tag,
                        "operator_id": op_id,
                        "name": op_data.get("name"),
                        "state": op_data.get("state"),
                        "progress": op_data.get("progress"),
                        "total": op_data.get("total"),
                        "total_rows": op_data.get("total_rows"),
                        "queued_blocks": op_data.get("queued_blocks"),
                    }

                    if "task_pool" in op_data:
                        operator_info["task_pool"] = op_data["task_pool"]

                    if "actor_pool" in op_data:
                        operator_info["actor_pool"] = op_data["actor_pool"]

                    result.append(operator_info)

            return self._json_response({"job_id": job_id, "operators": result})
        except Exception as e:
            logger.exception(f"Failed to get operators for job {job_id}")
            return self._json_response({"error": str(e)}, status=500)
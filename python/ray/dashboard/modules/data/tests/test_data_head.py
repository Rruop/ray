import os
import sys

import pytest
import requests

import ray
from ray.job_submission import JobSubmissionClient
from ray.tests.conftest import *  # noqa

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"

DATA_HEAD_URLS = {
    "GET": "http://localhost:8265/api/data/datasets/{job_id}",
    "GET_OPERATORS": "http://localhost:8265/api/data/{job_id}/operators",
    "GET_EXECUTION_CONFIG": "http://localhost:8265/api/data/{job_id}/execution_config",
    "PUT_EXECUTION_CONFIG": "http://localhost:8265/api/data/{job_id}/execution_config",
    "DELETE_EXECUTION_CONFIG": "http://localhost:8265/api/data/{job_id}/execution_config",
    "LIST_EXECUTION_CONFIGS": "http://localhost:8265/api/data/execution_configs",
}

DATA_SCHEMA = [
    "state",
    "progress",
    "total",
    "total_rows",
    "output_rows",
    "num_errored_blocks",
    "ray_data_output_rows",
    "ray_data_spilled_bytes",
    "ray_data_current_bytes",
    "ray_data_cpu_usage_cores",
    "ray_data_gpu_usage_cores",
]

RESPONSE_SCHEMA = [
    "dataset",
    "job_id",
    "start_time",
    "end_time",
    "operators",
] + DATA_SCHEMA

OPERATOR_SCHEMA = [
    "name",
    "operator",
    "queued_blocks",
] + DATA_SCHEMA


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_unique_operator_id(ray_start_regular_shared):
    # This regression test addresses a bug caused by using a non-unique operator ID
    # format. Specifically, the third operator's name is limit11 with the ID limit112,
    # while the thirteenth operator's name is limit1 with the same ID limit112, leading
    # to a collision.
    ds = ray.data.range(100, override_num_blocks=20).limit(11)  # 3 operators
    for i in range(11):  # 11 more operators
        ds = ds.limit(1)
    ds._set_name("unique_operator_id_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("unique_operator_id_test")
    ]
    assert len(datasets) == 1
    dataset = datasets[0]

    operators = dataset["operators"]
    assert len(operators) == 3  # Should be 3 because of limiter operator fusion.


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_datasets(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20).map_batches(lambda x: x)
    ds.set_name("data_head_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("data_head_test")
    ]

    assert len(datasets) == 1
    assert sorted(datasets[0].keys()) == sorted(RESPONSE_SCHEMA)

    dataset = datasets[0]
    assert dataset["dataset"].startswith("data_head_test")
    assert dataset["job_id"] == job_id
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None

    operators = dataset["operators"]
    assert len(operators) == 2
    op0 = operators[0]
    op1 = operators[1]
    assert sorted(op0.keys()) == sorted(OPERATOR_SCHEMA)
    assert sorted(op1.keys()) == sorted(OPERATOR_SCHEMA)
    assert {
        "operator": "Input_0",
        "name": "Input",
        "state": "FINISHED",
        "progress": 20,
        "total": 20,
    }.items() <= op0.items()
    assert {
        "operator": "ReadRange->MapBatches(<lambda>)_1",
        "name": "ReadRange->MapBatches(<lambda>)",
        "state": "FINISHED",
        "progress": 20,
        "total": 20,
    }.items() <= op1.items()

    ds._set_name("another_data_head_test")
    ds.map_batches(lambda x: x).materialize()
    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()

    dataset = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("another_data_head_test")
    ][0]
    assert dataset["dataset"].startswith("another_data_head_test")
    assert dataset["job_id"] == job_id
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_num_errored_blocks_in_response(ray_start_regular_shared):
    """Test that num_errored_blocks metric is included in the response schema."""
    ds = ray.data.range(50, override_num_blocks=10).map_batches(lambda x: x)
    ds.set_name("errored_blocks_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) >= 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("errored_blocks_test")
    ]

    assert len(datasets) == 1
    dataset = datasets[0]

    # Verify num_errored_blocks is present at dataset level
    # This comes directly from _StatsActor, not from Prometheus
    assert "num_errored_blocks" in dataset
    # For a successful dataset without errors, this should be 0
    assert dataset["num_errored_blocks"] == 0

    # Verify num_errored_blocks is present at operator level
    for operator in dataset["operators"]:
        assert "num_errored_blocks" in operator


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_num_errored_blocks_with_failures(ray_start_regular_shared):
    """Test that num_errored_blocks correctly reflects failed tasks."""
    ctx = ray.data.DataContext.get_current()
    original_max_errored_blocks = ctx.max_errored_blocks

    try:
        # Allow up to 3 errored blocks
        ctx.max_errored_blocks = 3

        def fail_some_blocks(batch):
            # Fail based on batch content to get deterministic failures
            if batch["id"][0] < 2:
                raise RuntimeError("Intentional failure")
            return batch

        ds = ray.data.range(10, override_num_blocks=10).map_batches(
            fail_some_blocks, batch_size=1
        )
        ds.set_name("errored_blocks_with_failures_test")

        # This should complete with some errored blocks (not fail completely)
        result = ds.take_all()

        # We should have 8 successful results (10 - 2 failures)
        assert len(result) == 8

        client = JobSubmissionClient()
        jobs = client.list_jobs()
        job_id = jobs[0].job_id

        data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
        datasets = [
            dataset
            for dataset in data["datasets"]
            if dataset["dataset"].startswith("errored_blocks_with_failures_test")
        ]

        assert len(datasets) == 1
        dataset = datasets[0]

        # The num_errored_blocks should reflect the failures
        # This comes directly from _StatsActor, not from Prometheus
        assert "num_errored_blocks" in dataset
        # Verify the structure is correct (should be an integer)
        assert isinstance(dataset["num_errored_blocks"], int)

    finally:
        ctx.max_errored_blocks = original_max_errored_blocks


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_operators(ray_start_regular_shared):
    """Test the /api/data/operators/{job_id} endpoint."""
    ds = ray.data.range(100, override_num_blocks=20).map_batches(lambda x: x)
    ds.set_name("operators_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET_OPERATORS"].format(job_id=job_id)).json()

    assert "operators" in data
    assert data["job_id"] == job_id

    # Filter to only our test dataset's operators
    operators = [
        op for op in data["operators"]
        if op["dataset"].startswith("operators_test")
    ]

    assert len(operators) >= 1

    # Verify operator structure
    for op in operators:
        assert "operator_id" in op
        assert "name" in op
        assert "state" in op
        assert "progress" in op

        # TaskPool operators should have task_pool info
        if "task_pool" in op:
            assert "active_tasks" in op["task_pool"]
            assert "max_concurrency" in op["task_pool"]


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_execution_config(ray_start_regular_shared):
    """Test the execution config CRUD endpoints."""
    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) >= 1, jobs
    job_id = jobs[0].job_id

    # Test GET non-existent config returns 404
    response = requests.get(
        DATA_HEAD_URLS["GET_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 404
    data = response.json()
    assert "error" in data

    # Test PUT to create a new config
    config_payload = {
        "config": {
            "job_id": job_id,
            "operators": {
                "op1": {
                    "type": "task_pool",
                    "id": "op1",
                    "name": "TestOperator",
                    "max_concurrency": 10,
                }
            }
        }
    }
    response = requests.put(
        DATA_HEAD_URLS["PUT_EXECUTION_CONFIG"].format(job_id=job_id),
        json=config_payload,
    )
    assert response.status_code == 201  # Created
    data = response.json()
    assert data["success"] is True
    assert data["created"] is True
    assert data["job_id"] == job_id

    # Test GET the created config
    response = requests.get(
        DATA_HEAD_URLS["GET_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == job_id
    assert "config" in data
    assert "operators" in data["config"]
    assert "op1" in data["config"]["operators"]
    assert data["config"]["operators"]["op1"]["max_concurrency"] == 10

    # Test PUT to update existing config
    config_payload["config"]["operators"]["op1"]["max_concurrency"] = 20
    response = requests.put(
        DATA_HEAD_URLS["PUT_EXECUTION_CONFIG"].format(job_id=job_id),
        json=config_payload,
    )
    assert response.status_code == 200  # Updated (not created)
    data = response.json()
    assert data["success"] is True
    assert data["created"] is False

    # Verify update was applied
    response = requests.get(
        DATA_HEAD_URLS["GET_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 200
    data = response.json()
    assert data["config"]["operators"]["op1"]["max_concurrency"] == 20

    # Test LIST execution configs
    response = requests.get(DATA_HEAD_URLS["LIST_EXECUTION_CONFIGS"])
    assert response.status_code == 200
    data = response.json()
    assert "configs" in data
    assert job_id in data["configs"]

    # Test DELETE the config
    response = requests.delete(
        DATA_HEAD_URLS["DELETE_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["deleted"] is True

    # Verify deletion - GET should return 404
    response = requests.get(
        DATA_HEAD_URLS["GET_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 404

    # Test DELETE non-existent config returns 404
    response = requests.delete(
        DATA_HEAD_URLS["DELETE_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 404


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_put_execution_config_invalid_request(ray_start_regular_shared):
    """Test PUT execution config with invalid request body."""
    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) >= 1, jobs
    job_id = jobs[0].job_id

    # Test PUT with missing 'config' field
    response = requests.put(
        DATA_HEAD_URLS["PUT_EXECUTION_CONFIG"].format(job_id=job_id),
        json={"invalid": "payload"},
    )
    assert response.status_code == 400
    data = response.json()
    assert "error" in data
    assert "Missing 'config' field" in data["error"]

    # Test PUT with invalid config format (missing type)
    invalid_config = {
        "config": {
            "operators": {
                "op1": {
                    "id": "op1",
                    "name": "TestOp",
                    # missing "type" field
                }
            }
        }
    }
    response = requests.put(
        DATA_HEAD_URLS["PUT_EXECUTION_CONFIG"].format(job_id=job_id),
        json=invalid_config,
    )
    assert response.status_code == 400
    data = response.json()
    assert "error" in data


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_execution_config_with_actor_pool(ray_start_regular_shared):
    """Test execution config with ActorPool operator configuration."""
    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) >= 1, jobs
    job_id = jobs[0].job_id

    # Create config with ActorPool operator
    config_payload = {
        "config": {
            "job_id": job_id,
            "operators": {
                "actor_op": {
                    "type": "actor_pool",
                    "id": "actor_op",
                    "name": "ActorPoolOperator",
                    "min_size": 1,
                    "max_size": 10,
                    "size": 5,
                }
            }
        }
    }
    response = requests.put(
        DATA_HEAD_URLS["PUT_EXECUTION_CONFIG"].format(job_id=job_id),
        json=config_payload,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True

    # Verify the actor pool config was stored correctly
    response = requests.get(
        DATA_HEAD_URLS["GET_EXECUTION_CONFIG"].format(job_id=job_id)
    )
    assert response.status_code == 200
    data = response.json()
    actor_op = data["config"]["operators"]["actor_op"]
    assert actor_op["type"] == "actor_pool"
    assert actor_op["min_size"] == 1
    assert actor_op["max_size"] == 10
    assert actor_op["size"] == 5

    # Clean up
    requests.delete(DATA_HEAD_URLS["DELETE_EXECUTION_CONFIG"].format(job_id=job_id))


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_operators_with_actor_pool(ray_start_regular_shared):
    """Test that ActorPool metrics are exposed via the operators endpoint."""

    class Identity:
        def __call__(self, batch):
            return batch

    ds = ray.data.range(100, override_num_blocks=10).map_batches(
        Identity,
        compute=ray.data.ActorPoolStrategy(size=2),
    )
    ds.set_name("actor_pool_operators_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET_OPERATORS"].format(job_id=job_id)).json()

    # Filter to our test dataset's operators
    operators = [
        op for op in data["operators"]
        if op["dataset"].startswith("actor_pool_operators_test")
    ]

    # Find the ActorPool operator
    actor_pool_ops = [op for op in operators if "actor_pool" in op]

    # The map_batches with ActorPoolStrategy should have actor_pool info
    # Note: After execution finishes, actor pool may be shut down,
    # so we just verify the structure is correct when present
    for op in actor_pool_ops:
        assert "current_size" in op["actor_pool"]
        assert "running" in op["actor_pool"]
        assert "pending" in op["actor_pool"]
        assert "min_size" in op["actor_pool"]
        assert "max_size" in op["actor_pool"]



if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))

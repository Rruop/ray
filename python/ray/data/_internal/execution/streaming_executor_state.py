"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
from ray.data._internal.execution.backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    Waitable,
    _ActorPoolInfo,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.ranker import Ranker
from ray.data._internal.execution.resource_manager import (
    ResourceManager,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.util import (
    unify_schemas_with_validation,
)

if TYPE_CHECKING:
    from ray.data.block import Schema

logger = logging.getLogger(__name__)

# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]

# Environment variable to enable detailed scheduling loop diagnostics
_ENABLE_SCHED_LOOP_DIAGNOSTICS = os.environ.get(
    "RAY_DATA_ENABLE_SCHED_LOOP_DIAGNOSTICS", "0"
) == "1"


@dataclass
class SchedulingLoopDiagnostics:
    """Diagnostics for a single scheduling loop iteration.

    This class collects detailed timing and count information to help
    identify performance bottlenecks in the scheduling loop.
    """

    # Timing metrics (in seconds)
    ray_wait_active_tasks_s: float = 0.0  # Time spent in ray.wait for active tasks
    prepare_metadata_total_s: float = 0.0  # Total time in prepare_metadata calls
    prepare_metadata_max_s: float = 0.0  # Max time for a single prepare_metadata
    ray_wait_meta_refs_s: float = 0.0  # Time spent in ray.wait for meta refs
    process_meta_total_s: float = 0.0  # Time processing metadata (ray.get + complete)
    pull_outputs_s: float = 0.0  # Time pulling outputs from operators

    # Count metrics
    num_active_tasks: int = 0  # Number of active tasks checked
    num_ready_tasks: int = 0  # Number of tasks ready from ray.wait
    num_prepare_metadata_calls: int = 0  # Number of prepare_metadata calls
    num_prepare_metadata_waited: int = 0  # Number that actually waited (took >1ms)
    num_pending_meta_tasks: int = 0  # Number of tasks with pending metadata
    num_meta_refs_ready: int = 0  # Number of meta refs ready after batch wait
    num_blocks_processed: int = 0  # Number of blocks processed

    # Derived metrics
    def total_time_s(self) -> float:
        """Total time accounted for in this iteration."""
        return (
            self.ray_wait_active_tasks_s
            + self.prepare_metadata_total_s
            + self.ray_wait_meta_refs_s
            + self.process_meta_total_s
            + self.pull_outputs_s
        )

    def to_log_string(self) -> str:
        """Format diagnostics for logging."""
        return (
            f"SchedulingLoopDiagnostics: "
            f"ray_wait_active={self.ray_wait_active_tasks_s * 1000:.1f}ms, "
            f"prepare_meta={self.prepare_metadata_total_s * 1000:.1f}ms "
            f"(calls={self.num_prepare_metadata_calls}, "
            f"waited={self.num_prepare_metadata_waited}, "
            f"max={self.prepare_metadata_max_s * 1000:.1f}ms), "
            f"ray_wait_meta={self.ray_wait_meta_refs_s * 1000:.1f}ms, "
            f"process_meta={self.process_meta_total_s * 1000:.1f}ms, "
            f"pull_outputs={self.pull_outputs_s * 1000:.1f}ms, "
            f"active_tasks={self.num_active_tasks}, "
            f"ready_tasks={self.num_ready_tasks}, "
            f"pending_meta={self.num_pending_meta_tasks}, "
            f"meta_ready={self.num_meta_refs_ready}, "
            f"blocks_processed={self.num_blocks_processed}, "
            f"total={self.total_time_s() * 1000:.1f}ms"
        )


# Global diagnostics for the last N iterations (ring buffer)
_DIAGNOSTICS_BUFFER_SIZE = 100
_diagnostics_buffer: List[SchedulingLoopDiagnostics] = []
_diagnostics_lock = threading.Lock()


def get_recent_diagnostics() -> List[SchedulingLoopDiagnostics]:
    """Get recent scheduling loop diagnostics for analysis."""
    with _diagnostics_lock:
        return list(_diagnostics_buffer)


def _record_diagnostics(diag: SchedulingLoopDiagnostics) -> None:
    """Record diagnostics to the ring buffer."""
    with _diagnostics_lock:
        _diagnostics_buffer.append(diag)
        if len(_diagnostics_buffer) > _DIAGNOSTICS_BUFFER_SIZE:
            _diagnostics_buffer.pop(0)


class OpBufferQueue:
    """A FIFO queue to buffer RefBundles between upstream and downstream operators.
    This class is thread-safe.
    """

    def __init__(self):
        self._num_blocks = 0
        self._queue = create_bundle_queue()
        self._num_per_split = defaultdict(int)
        self._lock = threading.Lock()
        # Used to buffer output RefBundles indexed by output splits.
        self._outputs_by_split = defaultdict(create_bundle_queue)
        super().__init__()

    @property
    def memory_usage(self) -> int:
        """The total memory usage of the queue in bytes."""
        with self._lock:
            # The split queues contain bundles popped from the main queue. So, a bundle
            # will either be in the main queue or in one of the split queues, and we
            # don't need to worry about double counting.
            return self._queue.estimate_size_bytes() + sum(
                split_queue.estimate_size_bytes()
                for split_queue in self._outputs_by_split.values()
            )

    @property
    def num_blocks(self) -> int:
        """The total number of blocks in the queue."""
        with self._lock:
            return self._num_blocks

    def __len__(self):
        with self._lock:
            return len(self._queue)

    def has_next(self, output_split_idx: Optional[int] = None) -> bool:
        """Whether next RefBundle is available.

        Args:
            output_split_idx: If specified, only check ref bundles with the
                given output split.
        """
        if output_split_idx is None:
            with self._lock:
                return len(self._queue) > 0
        else:
            with self._lock:
                return self._num_per_split[output_split_idx] > 0

    def append(self, ref: RefBundle):
        """Append a RefBundle to the queue."""
        with self._lock:
            self._queue.add(ref)
            self._num_blocks += len(ref.blocks)
            if ref.output_split_idx is not None:
                self._num_per_split[ref.output_split_idx] += 1

    def pop(self, output_split_idx: Optional[int] = None) -> Optional[RefBundle]:
        """Pop a RefBundle from the queue.
        Args:
            output_split_idx: If specified, only pop a RefBundle
                with the given output split.
        Returns:
            A RefBundle if available, otherwise None.
        """
        ret = None
        if output_split_idx is None:
            try:
                with self._lock:
                    ret = self._queue.get_next()
            except IndexError:
                pass
        else:
            with self._lock:
                split_queue = self._outputs_by_split[output_split_idx]
            if len(split_queue) == 0:
                # Move all ref bundles to their indexed queues
                # Note, the reason why we do indexing here instead of in the append
                # is because only the last `OpBufferQueue` in the DAG, which will call
                # pop with output_split_idx, needs indexing.
                # If we also index the `OpBufferQueue`s in the middle, we cannot
                # preserve the order of ref bundles with different output splits.
                with self._lock:
                    while len(self._queue) > 0:
                        ref = self._queue.get_next()
                        self._outputs_by_split[ref.output_split_idx].add(ref)
            try:
                ret = split_queue.get_next()
            except IndexError:
                pass
        if ret is None:
            return None
        with self._lock:
            self._num_blocks -= len(ret.blocks)
            if ret.output_split_idx is not None:
                self._num_per_split[ret.output_split_idx] -= 1
        return ret

    def clear(self):
        with self._lock:
            self._queue.clear()
            self._num_blocks = 0
            self._num_per_split.clear()


@dataclass
class OpSchedulingStatus:
    """The scheduling status of an operator.

    This will be updated each time when StreamingExecutor makes
    a scheduling decision, i.e., in each `select_operator_to_run`
    call.
    """

    # Whether the op was considered runnable in the last scheduling
    # decision.
    runnable: bool = False
    # Whether the resources were sufficient for the operator to run
    # in the last scheduling decision.
    under_resource_limits: bool = False


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[OpBufferQueue]):
        # Each input queue is connected to another operator's output queue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.input_queues: List[OpBufferQueue] = inqueues
        # The output queue is connected to another operator's input queue (same object).
        #
        # Note: this queue is also accessed concurrently from the consumer thread.
        # (in addition to the streaming executor thread). Hence, it must be a
        # thread-safe type such as `deque`.
        self.output_queue: OpBufferQueue = OpBufferQueue()
        self.op = op
        self.num_completed_tasks = 0
        self.inputs_done_called = False
        # Tracks whether `input_done` is called for each input op.
        self.input_done_called = [False] * len(op.input_dependencies)
        # Used for StreamingExecutor to signal exception or end of execution
        self._finished: bool = False
        self._exception: Optional[Exception] = None
        self._scheduling_status = OpSchedulingStatus()
        self._schema: Optional["Schema"] = None
        self._warned_on_schema_divergence: bool = False

    def __repr__(self):
        return f"OpState({self.op.name})"

    def has_pending_bundles(self) -> bool:
        return any(len(q) > 0 for q in self.input_queues)

    def total_enqueued_input_blocks(self) -> int:
        """Total number of blocks currently enqueued among:
        1. Input queue(s) pending dispatching (``OpState.input_queues``)
        2. Operator's internal queues (like ``MapOperator``s ref-bundler, etc)
        """
        external_queue_size = sum(q.num_blocks for q in self.input_queues)
        internal_queue_size = (
            self.op.internal_input_queue_num_blocks()
            if isinstance(self.op, InternalQueueOperatorMixin)
            else 0
        )
        return external_queue_size + internal_queue_size

    def total_enqueued_input_blocks_bytes(self) -> int:
        """Total number of bytes occupied by input bundles currently enqueued among:
        1. Input queue(s) pending dispatching (``OpState.input_queues``)
        2. Operator's internal queues (like ``MapOperator``s ref-bundler, etc)
        """
        internal_queue_size_bytes = (
            self.op.internal_input_queue_num_bytes()
            if isinstance(self.op, InternalQueueOperatorMixin)
            else 0
        )
        return self.input_queue_bytes() + internal_queue_size_bytes

    def total_enqueued_output_blocks(self) -> int:
        """Total number of blocks currently enqueued among:

        1. Output queue(s) pending dispatching (``OpState.output_queue``)
        2. Operator's internal output queues (like ``MapOperator``s reordering
        bundle-queue, when ``preserve_order=True`` etc)
        """
        external_queue_size = self.output_queue.num_blocks
        internal_queue_size = (
            self.op.internal_output_queue_num_blocks()
            if isinstance(self.op, InternalQueueOperatorMixin)
            else 0
        )

        return external_queue_size + internal_queue_size

    def total_enqueued_output_blocks_bytes(self) -> int:
        """Total number of bytes occupied by output bundles currently enqueued
        among:

        1. Output queue(s) pending dispatching (``OpState.output_queue``)
        2. Operator's internal output queues (like ``MapOperator``s reordering
        bundle-queue, when ``preserve_order=True`` etc)
        """
        internal_queue_size_bytes = (
            self.op.internal_output_queue_num_bytes()
            if isinstance(self.op, InternalQueueOperatorMixin)
            else 0
        )

        return self.output_queue_bytes() + internal_queue_size_bytes

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""

        ref, diverged = dedupe_schemas_with_validation(
            self._schema,
            ref,
            warn=not self._warned_on_schema_divergence,
            enforce_schemas=self.op.data_context.enforce_schemas,
        )

        self._schema = ref.schema
        self._warned_on_schema_divergence |= diverged

        self.output_queue.append(ref)
        self.num_completed_tasks += 1

        actor_info = self.op.get_actor_info()

        self.op.metrics.num_alive_actors = actor_info.running
        self.op.metrics.num_restarting_actors = actor_info.restarting
        self.op.metrics.num_pending_actors = actor_info.pending
        for next_op in self.op.output_dependencies:
            next_op.metrics.num_external_inqueue_blocks += len(ref.blocks)
            next_op.metrics.num_external_inqueue_bytes += ref.size_bytes()
        self.op.metrics.num_external_outqueue_blocks += len(ref.blocks)
        self.op.metrics.num_external_outqueue_bytes += ref.size_bytes()

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.input_queues):
            ref = inqueue.pop()
            if ref is not None:
                self.op.add_input(ref, input_index=i)
                self.op.metrics.num_external_inqueue_bytes -= ref.size_bytes()
                self.op.metrics.num_external_inqueue_blocks -= len(ref.blocks)
                input_op = self.op.input_dependencies[i]
                # TODO: This needs to be cleaned up.
                # the input_op's output queue = curr_op's input queue
                input_op.metrics.num_external_outqueue_blocks -= len(ref.blocks)
                input_op.metrics.num_external_outqueue_bytes -= ref.size_bytes()
                return

        assert False, "Nothing to dispatch"

    def get_output_blocking(self, output_split_idx: Optional[int]) -> RefBundle:
        """Get an item from this node's output queue, blocking as needed.

        Returns:
            The RefBundle from the output queue, or an error / end of stream indicator.

        Raises:
            StopIteration: If all outputs are already consumed.
            Exception: If there was an exception raised during execution.
        """
        while True:
            # Check if StreamingExecutor has caught an exception or is done execution.
            if self._exception is not None:
                raise self._exception
            elif self._finished and not self.output_queue.has_next(output_split_idx):
                raise StopIteration()
            ref = self.output_queue.pop(output_split_idx)
            if ref is not None:
                # Update outqueue metrics when blocks are removed from this operator's outqueue
                # TODO: Abstract queue-releated metrics to queue.
                self.op.metrics.num_external_outqueue_blocks -= len(ref.blocks)
                self.op.metrics.num_external_outqueue_bytes -= ref.size_bytes()
                return ref
            time.sleep(0.01)

    def input_queue_bytes(self) -> int:
        """Return the object store memory of this operator's inqueue."""
        total = 0
        for op, inq in zip(self.op.input_dependencies, self.input_queues):
            # Exclude existing input data items from dynamic memory usage.
            if not isinstance(op, InputDataBuffer):
                total += inq.memory_usage
        return total

    def output_queue_bytes(self) -> int:
        """Return the object store memory of this operator's outqueue."""
        return self.output_queue.memory_usage

    def mark_finished(self, exception: Optional[Exception] = None):
        """Marks this operator as finished. Used for exiting get_output_blocking."""
        if exception is None:
            self._finished = True
        else:
            self._exception = exception


def build_streaming_topology(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Topology:
    """Instantiate the streaming operator state topology for the given DAG.

    This involves creating the operator state for each operator in the DAG,
    registering it with this class, and wiring up the inqueues/outqueues of
    dependent operator states.

    Args:
        dag: The operator DAG to instantiate.
        options: The execution options to use to start operators.

    Returns:
        The topology dict holding the streaming execution state.
    """

    topology: Topology = {}

    # DFS walk to wire up operator states.
    def setup_state(op: PhysicalOperator) -> OpState:
        if op in topology:
            raise ValueError("An operator can only be present in a topology once.")

        # Wire up the input outqueues to this op's inqueues.
        inqueues = []
        for parent in op.input_dependencies:
            parent_state = setup_state(parent)
            inqueues.append(parent_state.output_queue)

        # Create state.
        op_state = OpState(op, inqueues)
        topology[op] = op_state
        op.start(options)
        return op_state

    setup_state(dag)
    return topology


def process_completed_tasks(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    max_errored_blocks: int,
) -> Tuple[Dict["OpState", int], Optional[SchedulingLoopDiagnostics]]:
    """Process any newly completed tasks. To update operator
    states, call `update_operator_states()` afterwards.

    Args:
        topology: The topology of operators.
        backpressure_policies: The backpressure policies to use.
        max_errored_blocks: Max number of errored blocks to allow,
            unlimited if negative.
    Returns:
        A tuple of:
        - A dict mapping OpState to the number of errored blocks for that operator.
        - Optional diagnostics if enabled via RAY_DATA_ENABLE_SCHED_LOOP_DIAGNOSTICS=1
    """
    # Initialize diagnostics if enabled
    diag = SchedulingLoopDiagnostics() if _ENABLE_SCHED_LOOP_DIAGNOSTICS else None

    # All active tasks, keyed by their waitables.
    active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    if diag:
        diag.num_active_tasks = len(active_tasks)

    max_bytes_to_read_per_op: Dict[OpState, int] = {}
    for op, state in topology.items():
        # Check all backpressure policies for max_task_output_bytes_to_read
        # Use the minimum limit from all policies (most restrictive)
        max_bytes_to_read = None
        # Track the first policy that limits output (returning 0 bytes)
        limiting_policy = None
        for policy in backpressure_policies:
            policy_limit = policy.max_task_output_bytes_to_read(op)
            if policy_limit is not None:
                if policy_limit == 0 and limiting_policy is None:
                    limiting_policy = policy.name
                if max_bytes_to_read is None:
                    max_bytes_to_read = policy_limit
                else:
                    max_bytes_to_read = min(max_bytes_to_read, policy_limit)

        # If no policy provides a limit, there's no limit
        op.notify_in_task_output_backpressure(max_bytes_to_read == 0, limiting_policy)
        if max_bytes_to_read is not None:
            max_bytes_to_read_per_op[state] = max_bytes_to_read

    # Process completed Ray tasks and notify operators.
    errored_blocks_per_op: Dict["OpState", int] = defaultdict(int)
    num_errored_blocks = 0
    if active_tasks:
        # ===== Phase 1: ray.wait for active tasks =====
        t_ray_wait_start = time.perf_counter()
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )
        if diag:
            diag.ray_wait_active_tasks_s = time.perf_counter() - t_ray_wait_start
            diag.num_ready_tasks = len(ready)

        # Organize tasks by the operator they belong to, and sort them by task index.
        # So that we'll process them in a deterministic order.
        # This is because backpressure policies may limit the number of blocks to read
        # per operator. In this case, we want to have fewer tasks finish quickly and
        # yield resources, instead of having all tasks output blocks together.
        ready_tasks_by_op = defaultdict(list)
        for ref in ready:
            state, task = active_tasks[ref]
            ready_tasks_by_op[state].append(task)

        # ========== Batch Metadata Fetching (Solution 2) ==========
        # The key optimization: instead of calling ray.get() with 1s timeout
        # sequentially for each task (N tasks × 1s = N seconds worst case),
        # we batch all metadata refs and use a single ray.wait() with short timeout.
        #
        # IMPORTANT: Each task may produce MULTIPLE blocks. The original on_data_ready()
        # uses a while loop to read all available blocks from a task's streaming generator.
        # Our batch approach processes ONE block per task per scheduling loop iteration.
        # This is acceptable because:
        # 1. Tasks with more blocks will be processed again in subsequent iterations
        # 2. The scheduling loop runs frequently
        # 3. The key bottleneck (serial 1s timeouts) is eliminated

        # ===== Phase 2: Collect metadata refs (prepare_metadata calls) =====
        # Step 1: Collect metadata refs and separate task types
        pending_meta_tasks = []  # [(state, task, meta_ref), ...]
        non_data_tasks = []  # [(state, task), ...] for MetadataOpTask

        for state, ready_tasks in ready_tasks_by_op.items():
            # Sort tasks by index (helps preserve_order case)
            ready_tasks = sorted(ready_tasks, key=lambda t: t.task_index())
            for task in ready_tasks:
                if isinstance(task, DataOpTask):
                    try:
                        # Prepare metadata ref without blocking
                        # This gets block_ref and meta_ref ready for batch waiting
                        t_prepare_start = time.perf_counter() if diag else 0
                        prepared = task.prepare_metadata()
                        if diag:
                            prepare_time = time.perf_counter() - t_prepare_start
                            diag.num_prepare_metadata_calls += 1
                            diag.prepare_metadata_total_s += prepare_time
                            diag.prepare_metadata_max_s = max(
                                diag.prepare_metadata_max_s, prepare_time
                            )
                            # Track if this call actually waited (>1ms)
                            if prepare_time > 0.001:
                                diag.num_prepare_metadata_waited += 1

                        if prepared:
                            meta_ref = task.get_pending_meta_ref()
                            if not meta_ref.is_nil():
                                pending_meta_tasks.append((state, task, meta_ref))
                        # If prepare_metadata() returns False, task either:
                        # - Has finished (StopIteration, handled internally)
                        # - Block/meta ref not yet available (will retry next loop)
                    except Exception as e:
                        errored_blocks_per_op[state] += 1
                        num_errored_blocks += 1
                        should_ignore = (
                            max_errored_blocks < 0
                            or max_errored_blocks >= num_errored_blocks
                        )
                        error_message = (
                            "An exception was raised from a task of "
                            f'operator "{state.op.name}". '
                            f"[num_errored_blocks={num_errored_blocks}]"
                        )
                        if should_ignore:
                            remaining = (
                                max_errored_blocks - num_errored_blocks
                                if max_errored_blocks >= 0
                                else "unlimited"
                            )
                            error_message += (
                                " Ignoring this exception with remaining"
                                f" max_errored_blocks={remaining}."
                            )
                            logger.error(error_message, exc_info=e)
                        else:
                            error_message += (
                                " Dataset execution will now abort."
                                " To ignore this exception and continue, set"
                                " DataContext.max_errored_blocks."
                            )
                            logger.exception(error_message)
                            raise e from None
                else:
                    assert isinstance(task, MetadataOpTask)
                    non_data_tasks.append((state, task))

        # Step 2: Process MetadataOpTasks immediately (they don't need batching)
        for state, task in non_data_tasks:
            task.on_task_finished()

        # ===== Phase 3: Batch wait for metadata refs =====
        # Step 3: Batch wait for all pending metadata refs
        if pending_meta_tasks:
            if diag:
                diag.num_pending_meta_tasks = len(pending_meta_tasks)

            from ray.data._internal.execution.interfaces.physical_operator import (
                METADATA_WAIT_TIMEOUT_S,
            )

            meta_refs = [item[2] for item in pending_meta_tasks]
            t_meta_wait_start = time.perf_counter() if diag else 0
            ready_meta_refs, _ = ray.wait(
                meta_refs,
                num_returns=len(meta_refs),
                timeout=METADATA_WAIT_TIMEOUT_S,  # Only wait 100ms total, not per task
                fetch_local=True,
            )
            if diag:
                diag.ray_wait_meta_refs_s = time.perf_counter() - t_meta_wait_start
                diag.num_meta_refs_ready = len(ready_meta_refs)

            ready_meta_set = set(ready_meta_refs)

            # ===== Phase 4: Process metadata =====
            # Step 4: Process tasks with ready metadata
            # Iterate in original order to preserve task_index ordering per state
            t_process_meta_start = time.perf_counter() if diag else 0
            for state, task, meta_ref in pending_meta_tasks:
                if meta_ref not in ready_meta_set:
                    # Metadata not ready yet, will retry in next scheduling loop.
                    # Task's pending refs remain set, so prepare_metadata() will
                    # return True immediately in next iteration.
                    continue

                # Check max_bytes_to_read limit before processing
                if state in max_bytes_to_read_per_op:
                    if max_bytes_to_read_per_op[state] <= 0:
                        # Skip due to backpressure. Task's pending refs remain set,
                        # will be processed in next scheduling loop.
                        continue

                try:
                    # Metadata is ready locally, ray.get won't block
                    meta_with_schema = ray.get(meta_ref, timeout=0)
                    bytes_read = task.complete_with_metadata(meta_with_schema)
                    if diag:
                        diag.num_blocks_processed += 1
                    if state in max_bytes_to_read_per_op:
                        max_bytes_to_read_per_op[state] -= bytes_read
                except Exception as e:
                    errored_blocks_per_op[state] += 1
                    num_errored_blocks += 1
                    should_ignore = (
                        max_errored_blocks < 0
                        or max_errored_blocks >= num_errored_blocks
                    )
                    error_message = (
                        "An exception was raised from a task of "
                        f'operator "{state.op.name}". '
                        f"[num_errored_blocks={num_errored_blocks}]"
                    )
                    if should_ignore:
                        remaining = (
                            max_errored_blocks - num_errored_blocks
                            if max_errored_blocks >= 0
                            else "unlimited"
                        )
                        error_message += (
                            " Ignoring this exception with remaining"
                            f" max_errored_blocks={remaining}."
                        )
                        logger.error(error_message, exc_info=e)
                    else:
                        error_message += (
                            " Dataset execution will now abort."
                            " To ignore this exception and continue, set"
                            " DataContext.max_errored_blocks."
                        )
                        logger.exception(error_message)
                        raise e from None

            if diag:
                diag.process_meta_total_s = time.perf_counter() - t_process_meta_start

    # ===== Phase 5: Pull outputs =====
    # Pull any operator outputs into the streaming op state.
    t_pull_start = time.perf_counter() if diag else 0
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())
    if diag:
        diag.pull_outputs_s = time.perf_counter() - t_pull_start

    # Log and record diagnostics
    if diag:
        if diag.total_time_s() > 0.1:  # Log if loop takes > 100ms
            logger.info(diag.to_log_string())
        _record_diagnostics(diag)

    return dict(errored_blocks_per_op), diag


def update_operator_states(topology: Topology) -> None:
    """Update operator states accordingly for newly completed tasks.
    Should be called after `process_completed_tasks()`."""

    for op, op_state in topology.items():

        # Call inputs_done() on ops where no more inputs are coming.
        if op_state.inputs_done_called:
            continue
        all_inputs_done = True
        for idx, dep in enumerate(op.input_dependencies):
            if dep.has_completed() and not topology[dep].output_queue:
                if not op_state.input_done_called[idx]:
                    op.input_done(idx)
                    op_state.input_done_called[idx] = True
            else:
                all_inputs_done = False

        if all_inputs_done:
            op.all_inputs_done()
            op_state.inputs_done_called = True

    # Traverse the topology in reverse topological order.
    # For each op, if all of its downstream operators have completed.
    # call mark_execution_finished() to also complete this op.
    for op, op_state in reversed(list(topology.items())):

        dependents_completed = len(op.output_dependencies) > 0 and all(
            dep.has_completed() for dep in op.output_dependencies
        )
        if dependents_completed:
            op.mark_execution_finished()

        # Drain external input queue if current operator is execution finished.
        # This is needed when the limit is reached, and `mark_execution_finished`
        # is called manually.
        if op.has_execution_finished():
            for input_queue in op_state.input_queues:
                # Drain input queue
                input_queue.clear()


def get_eligible_operators(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    *,
    ensure_liveness: bool,
) -> List[PhysicalOperator]:
    """This method returns all operators that are eligible for execution in the current state
    of the pipeline.

    Operator is considered eligible for execution iff:

        1. It's NOT completed
        2. It has at least 1 input block (in the input queue)
        3. It can accept new inputs
        4. It's not currently throttled (for task-submission)

    """

    dispatchable_ops: List[PhysicalOperator] = []
    # Filter to ops that are eligible for execution, ie ones that are
    #   - Dispatchable
    #   - Not throttled
    eligible_ops: List[PhysicalOperator] = []

    # Debug: collect reasons why operators are not eligible
    ineligible_reasons: Dict[str, List[str]] = {}

    for op, state in topology.items():
        # Operator is considered being in task-submission back-pressure if any
        # back-pressure policy is violated. Track the first triggered policy.
        triggered_policy = None
        for p in backpressure_policies:
            if not p.can_add_input(op):
                triggered_policy = p.name
                break
        in_backpressure = triggered_policy is not None

        op_runnable = False

        reasons = []

        # Check whether operator could start executing immediately:
        #   - It's not completed
        #   - It can accept at least one input
        #   - Its input queue has a valid bundle
        is_completed = op.has_completed()
        can_add = op.can_add_input()
        has_bundles = state.has_pending_bundles()

        if is_completed:
            reasons.append("completed")
        if not can_add:
            reasons.append(
                f"can_add_input=False(active_tasks={op.num_active_tasks()})")
        if not has_bundles:
            reasons.append(
                f"no_pending_bundles(input_queues_len={[len(q) for q in state.input_queues]})")

        if not is_completed and can_add and has_bundles:
            if not in_backpressure:
                op_runnable = True
                eligible_ops.append(op)
                logger.debug(
                    "[Scheduler] Op %s is ELIGIBLE: "
                    "completed=%s, can_add_input=%s, has_pending_bundles=%s, "
                    "active_tasks=%d, input_queue_sizes=%s",
                    op.name,
                    is_completed,
                    can_add,
                    has_bundles,
                    op.num_active_tasks(),
                    [len(q) for q in state.input_queues],
                )
            else:
                dispatchable_ops.append(op)
                reasons.append(f"backpressure({triggered_policy})")

        if reasons:
            ineligible_reasons[op.name] = reasons

        # Update scheduling status
        state._scheduling_status = OpSchedulingStatus(
            runnable=op_runnable,
            under_resource_limits=not in_backpressure,
        )

        # Signal whether op in backpressure for stats collections
        op.notify_in_task_submission_backpressure(in_backpressure,
                                                  triggered_policy)

    # Log ineligible operators for debugging
    if ineligible_reasons:
        logger.debug(
            "[Scheduler] Ineligible operators and reasons: %s",
            {k: v for k, v in ineligible_reasons.items() if "completed" not in v},
        )

    # To ensure liveness, allow at least 1 operator to schedule tasks regardless of
    # limits in case when topology is entirely idle (no active tasks running)
    if (
        not eligible_ops
        and ensure_liveness
        and all(op.num_active_tasks() == 0 for op in topology)
    ):
        logger.debug(
            "[Scheduler] No eligible ops, but ensure_liveness=True and all ops idle. "
            "Returning dispatchable_ops=%s for liveness.",
            [op.name for op in dispatchable_ops],
        )
        return dispatchable_ops

    return eligible_ops


def select_operator_to_run(
    topology: Topology,
    resource_manager: ResourceManager,
    backpressure_policies: List[BackpressurePolicy],
    ensure_liveness: bool,
    ranker: "Ranker",
) -> Optional[PhysicalOperator]:
    """Select next operator to launch new tasks.

    The objective of this method is to maximize the throughput of the overall
    pipeline, subject to defined memory, parallelism and other constraints.

    To achieve that this method implements following protocol:

        1. Collects all _eligible_ to run operators (check `_get_eligible_ops`
           for more details)
        2. Applies stack-ranking algorithm to select the best operator (check
           `_create_eligible_ops_ranker` for more details)

    """
    eligible_ops = get_eligible_operators(
        topology,
        backpressure_policies,
        ensure_liveness=ensure_liveness,
    )

    if not eligible_ops:
        return None

    ranks = ranker.rank_operators(eligible_ops, topology, resource_manager)

    assert len(eligible_ops) == len(ranks), (eligible_ops, ranks)

    next_op, _ = min(zip(eligible_ops, ranks), key=lambda t: t[1])

    return next_op


def _actor_info_summary_str(info: _ActorPoolInfo) -> str:
    total = info.running + info.pending + info.restarting
    base = f"Actors: {total}"

    if total == info.running:
        return base
    else:
        return f"{base} ({info})"


def dedupe_schemas_with_validation(
    old_schema: Optional["Schema"],
    bundle: "RefBundle",
    warn: bool = True,
    enforce_schemas: bool = False,
) -> Tuple["RefBundle", bool]:
    """Unify/Dedupe two schemas, warning if warn=True

    Args:
        old_schema: The old schema to unify. This can be `None`, in which case
            the new schema will be used as the old schema.
        bundle: The new `RefBundle` to unify with the old schema.
        warn: Raise a warning if the schemas diverge.
        enforce_schemas: If `True`, allow the schemas to diverge and return unified schema.
            If `False`, but keep the old schema.

    Returns:
        A ref bundle with the unified schema of the two input schemas.
    """

    # Note, often times the refbundles correspond to only one schema. We can reduce the
    # memory footprint of multiple schemas by keeping only one copy.
    diverged = False

    from ray.data.block import _is_empty_schema

    if _is_empty_schema(old_schema):
        return bundle, diverged

    # This check is fast assuming pyarrow schemas
    if old_schema == bundle.schema:
        return bundle, diverged

    diverged = True
    if warn and enforce_schemas:
        logger.warning(
            f"Operator produced a RefBundle with a different schema "
            f"than the previous one. Previous schema: {old_schema}, "
            f"new schema: {bundle.schema}. This may lead to unexpected behavior."
        )
    if enforce_schemas:
        old_schema = unify_schemas_with_validation([old_schema, bundle.schema])

    return (
        RefBundle(
            bundle.blocks,
            schema=old_schema,
            owns_blocks=bundle.owns_blocks,
            output_split_idx=bundle.output_split_idx,
            _cached_object_meta=bundle._cached_object_meta,
            _cached_preferred_locations=bundle._cached_preferred_locations,
        ),
        diverged,
    )


def format_op_state_summary(
    op_state: OpState, resource_manager: ResourceManager, verbose: bool = False
) -> str:
    """Get a formatted summary of the OpState for progress reporting."""
    # Active tasks with running/queued breakdown if available
    active = op_state.op.num_active_tasks()

    # Try to get task distribution (running vs queued) if the operator supports it
    if hasattr(op_state.op, "get_task_distribution"):
        try:
            estimated_running, estimated_queued = op_state.op.get_task_distribution()
            desc = f"Tasks: {active} (running={estimated_running}, queued={estimated_queued})"
        except Exception:
            # Fallback to simple format if get_task_distribution fails
            desc = f"Tasks: {active}"
    else:
        desc = f"Tasks: {active}"

    if (
        op_state.op._in_task_submission_backpressure
        or op_state.op._in_task_output_backpressure
    ):
        backpressure_types = []
        if op_state.op._in_task_submission_backpressure:
            # The op is backpressured from submitting new tasks.
            policy = op_state.op._task_submission_backpressure_policy or ""
            backpressure_types.append(f"tasks({policy})")
        if op_state.op._in_task_output_backpressure:
            # The op is backpressured from producing new outputs.
            policy = op_state.op._task_output_backpressure_policy or ""
            backpressure_types.append(f"outputs({policy})")
        desc += f" [backpressured:{','.join(backpressure_types)}]"

    # Actors info
    desc += f"; {_actor_info_summary_str(op_state.op.get_actor_info())}"

    # Queued blocks
    desc += f"; Queued blocks: {op_state.total_enqueued_input_blocks()} ({memory_string(op_state.total_enqueued_input_blocks_bytes())})"
    desc += f"; Resources: {resource_manager.get_op_usage_str(op_state.op, verbose=verbose)}"

    # Any additional operator specific information.
    suffix = op_state.op.progress_str()
    if suffix:
        desc += f"; {suffix}"

    return desc

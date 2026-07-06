"""
Ray Data Perflog 打点统一接口。

将分散在 `streaming_executor.py` 和 `map_operator.py` 等文件里的 perflog
打点逻辑集中到本模块，统一封装：
1. session_name / submission_id 上下文（PerfContext）
2. RayPerfLogger.instance() 的获取与异常兜底
3. 业务语义化的 emit_xxx 函数

打点维度规范：
- `op_*`     : Operator 级，extra3 = op_name
- `worker_*` : Worker 进程级，extra3 = node_id
- `job_*`    : Job 级，extra3 = owner（KML_CREATOR）
- 集群级    : 仅传 session（如 obj_store_used、gpu_peak）
"""

from __future__ import annotations

import logging
import os
import resource as _resource
import threading as _threading
import time as _time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional

from ray.data._internal.execution.perf_logger import RayPerfLogger

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Context
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class PerfContext:
    """Perflog 打点上下文，承载 session / submission_id / 可选 owner。"""

    session: str
    submission_id: str
    owner: str = ""

    @classmethod
    def from_runtime(
        cls,
        submission_id: Optional[str] = None,
        session: Optional[str] = None,
    ) -> "PerfContext":
        """从 ray runtime context 自动解析 session / submission_id。

        允许调用方传入已知值跳过解析（如 streaming_executor 已有自己的解析逻辑）。
        """
        if session is None:
            try:
                import ray
                session = ray.get_runtime_context().get_session_name()
            except Exception:
                session = "unknown"

        if submission_id is None:
            try:
                import ray
                import ray._private.worker as _ray_worker

                _job_meta = dict(
                    _ray_worker.global_worker.core_worker.get_job_config().metadata
                )
                _job_id = ray.get_runtime_context().get_job_id()
                submission_id = _job_meta.get("job_submission_id", _job_id)
            except Exception:
                submission_id = "unknown"

        owner = os.environ.get("KML_CREATOR", "")
        return cls(session=session, submission_id=submission_id, owner=owner)


# ─────────────────────────────────────────────────────────────────────────────
# 内部工具
# ─────────────────────────────────────────────────────────────────────────────
def _safe_log(name: str, *args, micros: int = 0, count: int = 1) -> None:
    """统一封装：获取 RayPerfLogger 单例 + try/except 兜底。"""
    try:
        RayPerfLogger.instance().logstash(name, *args, micros=micros, count=count)
    except Exception as _e:
        # 打点失败不影响主流程，仅记录 warning
        logger.warning("[perf_metrics] _safe_log failed tag=%s: %s", name, _e, exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# Operator 级（extra3 = op_name）
# ─────────────────────────────────────────────────────────────────────────────
def emit_op_output(
    ctx: PerfContext, op_name: str, *, micros: int, count: int
) -> None:
    """operator 输出（每 task 完成后）"""
    _safe_log("operator_output", ctx.session, ctx.submission_id, op_name,
              micros=micros, count=count)


def emit_op_cpu(ctx: PerfContext, op_name: str, *, micros: int) -> None:
    """operator CPU 耗时（micros 为 cpu_us）"""
    _safe_log("operator_cpu", ctx.session, ctx.submission_id, op_name,
              micros=micros, count=1)


def emit_op_cpu_util(ctx: PerfContext, op_name: str, *, avg_cores: float) -> None:
    """operator CPU 利用率（micros 存 avg_cores*1000）"""
    _safe_log("operator_cpu_util", ctx.session, ctx.submission_id, op_name,
              micros=int(avg_cores * 1000), count=1)


def emit_op_threads(ctx: PerfContext, op_name: str, *, threads: int) -> None:
    """operator 线程数"""
    _safe_log("operator_threads", ctx.session, ctx.submission_id, op_name,
              micros=0, count=threads)


def emit_op_input(ctx: PerfContext, op_name: str, *, count: int) -> None:
    """operator 输入行数"""
    _safe_log("operator_input", ctx.session, ctx.submission_id, op_name,
              micros=0, count=count)


def emit_op_gpu_cur(ctx: PerfContext, op_name: str, *, gpu: float) -> None:
    """operator 当前 GPU 卡数（实时）"""
    _safe_log("op_gpu_cur", ctx.session, ctx.submission_id, op_name,
              micros=int(gpu * 1000), count=1)


def emit_op_gpu_peak(
    ctx: PerfContext, op_name: str, *, peak: float, samples: int
) -> None:
    """operator GPU 峰值（汇总）"""
    _safe_log("op_gpu_peak", ctx.session, ctx.submission_id, op_name,
              micros=int(peak * 1000), count=samples)


def emit_op_gpu_avg(
    ctx: PerfContext, op_name: str, *, avg: float, samples: int
) -> None:
    """operator GPU 平均（汇总）"""
    _safe_log("op_gpu_avg", ctx.session, ctx.submission_id, op_name,
              micros=int(avg * 1000), count=samples)


def emit_op_queue_cur(ctx: PerfContext, op_name: str, *, q_len: int) -> None:
    """operator 当前 pending 队列长度（实时）"""
    _safe_log("op_queue_cur", ctx.session, ctx.submission_id, op_name,
              micros=q_len * 1000, count=1)


def emit_op_queue_peak(
    ctx: PerfContext, op_name: str, *, peak: int, samples: int
) -> None:
    """operator 队列峰值（汇总）"""
    _safe_log("op_queue_peak", ctx.session, ctx.submission_id, op_name,
              micros=peak * 1000, count=samples)


def emit_op_queue_avg(
    ctx: PerfContext, op_name: str, *, avg: float, samples: int
) -> None:
    """operator 队列均值（汇总）"""
    _safe_log("op_queue_avg", ctx.session, ctx.submission_id, op_name,
              micros=int(avg * 1000), count=samples)


def emit_op_concurrency(ctx: PerfContext, op_name: str, *, concurrency: int) -> None:
    """operator 当前并发度（active tasks）"""
    _safe_log("op_concurrency", ctx.session, ctx.submission_id, op_name,
              micros=concurrency * 1000, count=1)


def emit_op_throughput(
    ctx: PerfContext, op_name: str, *, rows_per_s: float
) -> None:
    """operator 实时吞吐（rows/s）"""
    _safe_log("op_throughput", ctx.session, ctx.submission_id, op_name,
              micros=int(rows_per_s * 1000), count=1)


def emit_op_task_latency(
    ctx: PerfContext, op_name: str, *, latency_s: float, count: int = 1
) -> None:
    """operator 平均 task 耗时（实时）"""
    _safe_log("op_task_latency", ctx.session, ctx.submission_id, op_name,
              micros=int(latency_s * 1000), count=count)


def emit_op_task_latency_final(
    ctx: PerfContext, op_name: str, *, latency_s: float, finished: int
) -> None:
    """operator 平均 task 耗时（最终汇总）"""
    _safe_log("op_task_latency_final", ctx.session, ctx.submission_id, op_name,
              micros=int(latency_s * 1000), count=finished)


def emit_op_task_failed(
    ctx: PerfContext, op_name: str, *, delta: int
) -> None:
    """operator 失败 task 增量（实时）"""
    _safe_log("op_task_failed", ctx.session, ctx.submission_id, op_name,
              micros=0, count=delta)


def emit_op_task_failed_total(
    ctx: PerfContext, op_name: str, *, total: int
) -> None:
    """operator 失败 task 总数（最终汇总）"""
    _safe_log("op_task_failed_total", ctx.session, ctx.submission_id, op_name,
              micros=0, count=total)


def emit_op_obj_store(
    ctx: PerfContext, op_name: str, *, bytes_used: int
) -> None:
    """operator object store 使用量（micros 单位为 KB）"""
    _safe_log("op_obj_store_used", ctx.session, ctx.submission_id, op_name,
              micros=int(bytes_used // 1024), count=1)


def emit_op_block_error(
    ctx: PerfContext, op_name: str, *, count: int
) -> None:
    """operator 失败 block 数"""
    _safe_log("block_error", ctx.session, ctx.submission_id, op_name,
              micros=0, count=count)


def emit_op_read_latency(
    ctx: PerfContext, op_name: str, *, micros: int
) -> None:
    """Read 算子单 block 读耗时（plan_read_op 用）"""
    _safe_log("read_latency", ctx.session, ctx.submission_id, op_name,
              micros=micros, count=1)


# ─────────────────────────────────────────────────────────────────────────────
# Job 终态打点（dashboard/job/common.py 用）
# ─────────────────────────────────────────────────────────────────────────────
def emit_job_status(ctx: PerfContext, *, status: str) -> None:
    """Job 终态打点（extra3 = "owner|status"）。

    owner 从 ctx.owner 读取（如果为空，调用方可预先从 KML_CREATOR 读取填入 ctx）。
    """
    extra3 = f"{ctx.owner}|{status}"
    _safe_log("status", ctx.session, ctx.submission_id, extra3,
              micros=0, count=1)


def emit_job_duration(
    ctx: PerfContext, *, status: str, duration_ms: int
) -> None:
    """Job 持续时长打点（extra3 = status）"""
    _safe_log("job_duration", ctx.session, ctx.submission_id, status,
              micros=duration_ms * 1000, count=1)


# ─────────────────────────────────────────────────────────────────────────────
# Worker 级（extra3 = node_id）
# ─────────────────────────────────────────────────────────────────────────────
def emit_worker_rss(ctx: PerfContext, node_id: str, *, rss_bytes: int) -> None:
    """worker 节点 RSS（micros 单位为 KB）"""
    _safe_log("worker_rss", ctx.session, ctx.submission_id, node_id,
              micros=rss_bytes // 1024, count=1)


def emit_worker_cpu(ctx: PerfContext, node_id: str, *, micros: int) -> None:
    """worker 节点 CPU 耗时"""
    _safe_log("worker_cpu", ctx.session, ctx.submission_id, node_id,
              micros=micros, count=1)


def emit_worker_output_rows(ctx: PerfContext, node_id: str, *, count: int) -> None:
    """worker 节点输出行数"""
     if count <= 0:
        return
    _safe_log("worker_output_rows", ctx.session, ctx.submission_id, node_id,
              micros=0, count=count)


# ─────────────────────────────────────────────────────────────────────────────
# Job 级（extra3 = owner）
# ─────────────────────────────────────────────────────────────────────────────
def emit_job_input_rows_rt(ctx: PerfContext, *, delta: int) -> None:
    """Job 级输入行数增量"""
    if delta <= 0:
        return 
    _safe_log("job_input_rows_rt", ctx.session, ctx.submission_id, ctx.owner,
              micros=0, count=delta)


def emit_job_output_rows_rt(ctx: PerfContext, *, delta: int) -> None:
    """Job 级输出行数增量"""
    if delta <= 0:
        return 
    _safe_log("job_output_rows_rt", ctx.session, ctx.submission_id, ctx.owner,
              micros=0, count=delta)


def emit_job_obj_store(ctx: PerfContext, *, bytes_used: int) -> None:
    """Job 级 object store 使用量（micros 单位为 KB）"""
    _safe_log("job_obj_store_used", ctx.session, ctx.submission_id, ctx.owner,
              micros=int(bytes_used // 1024), count=1)


# ─────────────────────────────────────────────────────────────────────────────
# 集群级
# ─────────────────────────────────────────────────────────────────────────────
def emit_cluster_obj_store(ctx: PerfContext, *, bytes_used: int) -> None:
    """集群级 object store 使用量（micros 单位为 KB）。

    注意：原代码 `obj_store_used` 只传 session 一个 extra，不带 submission_id。
    """
    _safe_log("obj_store_used", ctx.session,
              micros=int(bytes_used // 1024), count=1)


def emit_cluster_gpu_cur(ctx: PerfContext, *, gpu: float) -> None:
    """集群级当前 GPU 卡数"""
    _safe_log("gpu_cur", ctx.session, ctx.submission_id,
              micros=int(gpu * 1000), count=1)


def emit_cluster_gpu_peak(ctx: PerfContext, *, peak: float, samples: int) -> None:
    """集群级 GPU 峰值（汇总）"""
    _safe_log("gpu_peak", ctx.session, ctx.submission_id,
              micros=int(peak * 1000), count=samples)


def emit_cluster_gpu_avg(ctx: PerfContext, *, avg: float, samples: int) -> None:
    """集群级 GPU 平均（汇总）"""
    _safe_log("gpu_avg", ctx.session, ctx.submission_id,
              micros=int(avg * 1000), count=samples)


def emit_cluster_mem_peak_rss(
    ctx: PerfContext, *, peak_rss_bytes: int, samples: int
) -> None:
    """driver 进程内存峰值（汇总，micros 单位为 KB）"""
    _safe_log("mem_peak_rss", ctx.session, ctx.submission_id,
              micros=peak_rss_bytes // 1024, count=samples)


def emit_cluster_mem_avg_rss(
    ctx: PerfContext, *, avg_rss_bytes: int, samples: int
) -> None:
    """driver 进程内存均值（汇总，micros 单位为 KB）"""
    _safe_log("mem_avg_rss", ctx.session, ctx.submission_id,
              micros=avg_rss_bytes // 1024, count=samples)


# ─────────────────────────────────────────────────────────────────────────────
# Task 注册（实时 CPU 采样线程）
# ─────────────────────────────────────────────────────────────────────────────
def register_task(ctx: PerfContext, op_name: str, node_id: str) -> None:
    """注册 task 到 perf_logger 实时 CPU 采样线程"""
    try:
        RayPerfLogger.instance().register_task(
            ctx.session, ctx.submission_id, op_name, node_id
        )
    except Exception as e:
        logger.warning(
            "[perf_metrics] register_task failed op=%s node=%s: %s",
            op_name, node_id, e, exc_info=True,
        )


def unregister_task(op_name: str) -> None:
    """注销 task，停止该 op 的实时 CPU 采样"""
    try:
        RayPerfLogger.instance().unregister_task(op_name)
    except Exception as e:
        logger.warning(
            "[perf_metrics] unregister_task failed op=%s: %s",
            op_name, e,
        )


# ═════════════════════════════════════════════════════════════════════════════
# 累加器 / 计时器（实例化使用，每次 dataset 执行新建一个）
# ═════════════════════════════════════════════════════════════════════════════
@dataclass
class RunningStats:
    """通用累加器：peak / sum / count，支持 avg 查询。"""

    peak: float = 0.0
    sum: float = 0.0
    count: int = 0

    def observe(self, v: float) -> None:
        self.sum += v
        self.count += 1
        if v > self.peak:
            self.peak = v

    @property
    def avg(self) -> float:
        return self.sum / self.count if self.count > 0 else 0.0


class DeltaTracker:
    """维护 last 值，自动算 delta。"""

    def __init__(self, initial: int = 0) -> None:
        self._last = initial

    def delta(self, current: int) -> int:
        d = current - self._last
        self._last = current
        return d


class OpStatsAccumulator:
    """多 op 维度的累积：gpu / queue / failed / rows_last。"""

    def __init__(self) -> None:
        self.gpu: Dict[str, RunningStats] = defaultdict(RunningStats)
        self.queue: Dict[str, RunningStats] = defaultdict(RunningStats)
        self.failed: Dict[str, DeltaTracker] = defaultdict(DeltaTracker)
        self.rows_last: Dict[str, int] = {}


_HAS_RUSAGE_THREAD = hasattr(_resource, "RUSAGE_THREAD")


class TaskTimer:
    """task 级 CPU/elapsed/线程数计时上下文管理器。

    使用：
        with TaskTimer() as timer:
            ...  # 业务逻辑
        timer.elapsed_us / timer.cpu_us / timer.avg_cores / timer.threads_after
    """

    def __init__(self) -> None:
        self.start_us: int = 0
        self.elapsed_us: int = 0
        self.cpu_us: int = 0
        self.threads_before: int = 0
        self.threads_after: int = 0
        self._ru_before: Optional[_resource.struct_rusage] = None

    def __enter__(self) -> "TaskTimer":
        self.start_us = int(_time.monotonic() * 1_000_000)
        if _HAS_RUSAGE_THREAD:
            self._ru_before = _resource.getrusage(_resource.RUSAGE_THREAD)
        self.threads_before = _threading.active_count()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: D401
        self.elapsed_us = int(_time.monotonic() * 1_000_000) - self.start_us
        ru_before = self._ru_before
        if ru_before is not None:
            ru_after = _resource.getrusage(_resource.RUSAGE_THREAD)
            self.cpu_us = int(
            (ru_after.ru_utime + ru_after.ru_stime
             - ru_before.ru_utime - ru_before.ru_stime) * 1_000_000
            )
        else:
            self.cpu_us = 0
        self.threads_after = _threading.active_count()
        # 不吞异常

    @property
    def avg_cores(self) -> float:
        return self.cpu_us / self.elapsed_us if self.elapsed_us > 0 else 0.0


# ═════════════════════════════════════════════════════════════════════════════
# 工具函数
# ═════════════════════════════════════════════════════════════════════════════
def get_node_id() -> str:
    """获取当前 worker 的短 node_id（hex 截取 16 字符）"""
    try:
        import ray._private.worker as _ray_worker

        return _ray_worker.global_worker.worker_id.hex()[:16]
    except Exception:
        return "unknown"


# ═════════════════════════════════════════════════════════════════════════════
# 高阶采集函数（采集 + 打点合一，业务文件直接调用）
# ═════════════════════════════════════════════════════════════════════════════
def sample_cluster_realtime(
    ctx: PerfContext,
    mem_stats: RunningStats,
    gpu_stats: RunningStats,
    *,
    rss_bytes: int,
) -> None:
    """采样集群级实时指标 + 累加 mem/gpu peak。

    注意：cluster gpu 总量需要从 topology 计算，建议另外调 sample_op_realtime。
    本函数只负责：
      - 累加 driver mem (rss_bytes) 到 mem_stats
      - 打点 obj_store_used（cluster 维度）
    """
    mem_stats.observe(rss_bytes)
    try:
        import ray

        cluster_res = ray.cluster_resources()
        avail_res = ray.available_resources()
        obj_total = cluster_res.get("object_store_memory", 0)
        obj_avail = avail_res.get("object_store_memory", 0)
        obj_used = max(0, obj_total - obj_avail)
        emit_cluster_obj_store(ctx, bytes_used=obj_used)
    except Exception:
        pass


def sample_job_realtime(
    ctx: PerfContext,
    topology,
    input_tracker: DeltaTracker,
    output_tracker: DeltaTracker,
) -> None:
    """采样 job 级 obj store + 行数 delta 并打点。"""
    # Job 级 object store 使用量
    try:
        job_obj_store = sum(
            (op.metrics.obj_store_mem_used or 0) for op in topology
        )
        emit_job_obj_store(ctx, bytes_used=job_obj_store)
    except Exception:
        pass

    # Job 级输入/输出行数 delta
    try:
        job_inputs_now = sum(
            (op.metrics.num_row_inputs_received or 0) for op in topology
        )
        job_outputs_now = sum(
            (op.metrics.rows_task_outputs_generated or 0) for op in topology
        )
        emit_job_input_rows_rt(ctx, delta=input_tracker.delta(job_inputs_now))
        emit_job_output_rows_rt(ctx, delta=output_tracker.delta(job_outputs_now))
    except Exception:
        pass


def sample_op_realtime(
    ctx: PerfContext,
    topology,
    accum: OpStatsAccumulator,
    gpu_stats: RunningStats,
    elapsed_s: float,
) -> None:
    """采样 operator 级实时指标：gpu / queue / concurrency / throughput / obj_store。

    顺便累加 cluster 级 gpu 总量到 gpu_stats，并打点 gpu_cur。
    """
    # 全局 GPU 采样：累加每个 op 的 num_gpus * active_tasks
    try:
        gpu_now = 0.0
        for op in topology:
            op_gpu_per_task = (
                getattr(op, "_ray_remote_args", {}).get("num_gpus", 0) or 0
            )
            if op_gpu_per_task > 0:
                gpu_now += op_gpu_per_task * op.num_active_tasks()
        gpu_stats.observe(gpu_now)
        emit_cluster_gpu_cur(ctx, gpu=gpu_now)
    except Exception:
        pass

    # 算子粒度 GPU 采样（peak/sum/count + 实时打点）
    try:
        for op in topology:
            op_name = op.name
            op_gpu_per_task = (
                getattr(op, "_ray_remote_args", {}).get("num_gpus", 0) or 0
            )
            op_gpu = op_gpu_per_task * op.num_active_tasks()
            accum.gpu[op_name].observe(op_gpu)
            emit_op_gpu_cur(ctx, op_name, gpu=op_gpu)
    except Exception:
        pass

    # 算子级 object store
    try:
        for op in topology:
            emit_op_obj_store(ctx, op.name,
                              bytes_used=(op.metrics.obj_store_mem_used or 0))
    except Exception:
        pass

    # 算子粒度 pending queue + concurrency + throughput
    for op, state in topology.items():
        op_name = op.name
        try:
            q_len = state.total_enqueued_input_blocks()
            accum.queue[op_name].observe(q_len)
            emit_op_queue_cur(ctx, op_name, q_len=q_len)
            emit_op_concurrency(ctx, op_name, concurrency=op.num_active_tasks())
        except Exception:
            pass
        # 吞吐
        try:
            cur_rows = op.metrics.row_outputs_taken
            last_rows = accum.rows_last.get(op_name, cur_rows)
            accum.rows_last[op_name] = cur_rows
            if elapsed_s > 0 and cur_rows > last_rows:
                rows_per_s = (cur_rows - last_rows) / elapsed_s
                emit_op_throughput(ctx, op_name, rows_per_s=rows_per_s)
        except Exception:
            pass


def sample_op_task_metrics(
    ctx: PerfContext,
    topology,
    accum: OpStatsAccumulator,
) -> None:
    """采样 operator 级 task 耗时 + 失败计数 delta，并打点。"""
    try:
        for op, _state in topology.items():
            op_name = op.name
            avg_latency = op.metrics.average_total_task_completion_time_s
            if avg_latency is not None:
                emit_op_task_latency(ctx, op_name, latency_s=avg_latency)

            failed_now = op.metrics.num_tasks_failed
            failed_delta = accum.failed[op_name].delta(failed_now)
            if failed_delta > 0:
                emit_op_task_failed(ctx, op_name, delta=failed_delta)
    except Exception as e:
        logger.warning("[PerfTaskSample] sampling failed: %s", e)


def flush_final(
    ctx: PerfContext,
    mem_stats: RunningStats,
    gpu_stats: RunningStats,
    accum: OpStatsAccumulator,
    topology,
) -> None:
    """finally 里的汇总打点：mem / gpu / queue 汇总 + op task 汇总。

    设计：各个维度独立判断 count > 0，避免某个维度无数据时跨维度连坐丢失。
    """
    # driver 进程内存汇总
    if mem_stats.count > 0:
        logger.warning(
            "[MemMonitor] submission_id=%s peak_rss_mb=%.1f avg_rss_mb=%.1f samples=%d",
            ctx.submission_id,
            mem_stats.peak / 1024 / 1024,
            mem_stats.avg / 1024 / 1024,
            mem_stats.count,
        )
        emit_cluster_mem_peak_rss(ctx,
                                  peak_rss_bytes=int(mem_stats.peak),
                                  samples=mem_stats.count)
        emit_cluster_mem_avg_rss(ctx,
                                 avg_rss_bytes=int(mem_stats.avg),
                                 samples=mem_stats.count)

    # GPU 汇总
    if gpu_stats.count > 0:
        logger.warning(
            "[GpuMonitor] submission_id=%s peak_gpu=%.1f avg_gpu=%.2f samples=%d",
            ctx.submission_id,
            gpu_stats.peak,
            gpu_stats.avg,
            gpu_stats.count,
        )
        emit_cluster_gpu_peak(ctx, peak=gpu_stats.peak, samples=gpu_stats.count)
        emit_cluster_gpu_avg(ctx, avg=gpu_stats.avg, samples=gpu_stats.count)

    # 算子粒度 GPU 汇总
    for op_name, stats in accum.gpu.items():
        if stats.count == 0:
            continue
        logger.warning(
            "[GpuMonitor] op=%s submission_id=%s peak_gpu=%.1f avg_gpu=%.2f",
            op_name, ctx.submission_id, stats.peak, stats.avg,
        )
        emit_op_gpu_peak(ctx, op_name, peak=stats.peak, samples=stats.count)
        emit_op_gpu_avg(ctx, op_name, avg=stats.avg, samples=stats.count)

    # 算子粒度 pending queue 汇总
    for op_name, qstats in accum.queue.items():
        if qstats.count == 0:
            continue
        logger.warning(
            "[QueueMonitor] op=%s submission_id=%s peak_pending=%d avg_pending=%.2f",
            op_name, ctx.submission_id, int(qstats.peak), qstats.avg,
        )
        emit_op_queue_peak(ctx, op_name,
                           peak=int(qstats.peak), samples=qstats.count)
        emit_op_queue_avg(ctx, op_name, avg=qstats.avg, samples=qstats.count)

    # 算子粒度 task latency / failed 汇总
    for op in topology:
        op_name = op.name
        try:
            avg_lat = op.metrics.average_total_task_completion_time_s
            finished = op.metrics.num_tasks_finished or 1
            if avg_lat is not None:
                emit_op_task_latency_final(ctx, op_name,
                                           latency_s=avg_lat, finished=finished)
        except Exception:
            pass
        try:
            total_failed = op.metrics.num_tasks_failed
            if total_failed > 0:
                emit_op_task_failed_total(ctx, op_name, total=total_failed)
        except Exception:
            pass


def emit_task_completion(
    ctx: PerfContext,
    op_name: str,
    node_id: str,
    *,
    timer: TaskTimer,
    input_rows: int,
    output_rows: int,
) -> None:
    """map task 完成时统一打点（operator + worker 维度）。"""
    if output_rows > 0:
        emit_op_output(ctx, op_name,
                       micros=timer.elapsed_us, count=output_rows)
    emit_op_cpu(ctx, op_name, micros=timer.cpu_us)
    emit_op_cpu_util(ctx, op_name, avg_cores=timer.avg_cores)
    emit_op_threads(ctx, op_name, threads=timer.threads_after)
    if input_rows > 0:
        emit_op_input(ctx, op_name, count=input_rows)

    # worker 维度
    try:
        import psutil as _psutil

        worker_rss = _psutil.Process().memory_info().rss
        emit_worker_rss(ctx, node_id, rss_bytes=worker_rss)
        emit_worker_cpu(ctx, node_id, micros=timer.cpu_us)
        emit_worker_output_rows(ctx, node_id, count=output_rows)
    except Exception:
        pass


# ═════════════════════════════════════════════════════════════════════════════
# PerfSampler：流式执行打点采样器
# ═════════════════════════════════════════════════════════════════════════════
class PerfSampler:
    """流式执行打点采样器，封装所有"数据获取 + 累加器 + 上下文构建"。

    业务文件（streaming_executor）只负责：
        1. 创建 sampler
        2. 维护 loop_counter
        3. `if counter % interval == 0: sampler.sample_xxx(topology)`
        4. finally: `sampler.flush(topology)`

    使用示例::

        sampler = PerfSampler(submission_id_provider=self._get_job_submission_id_or_job_id)
        try:
            while True:
                ...
                if mem_counter % 10 == 0:
                    sampler.sample_realtime(topology)
                if task_counter % 5 == 0:
                    sampler.sample_task_metrics(topology)
        finally:
            sampler.flush(topology)
    """

    def __init__(self, submission_id_provider=None) -> None:
        """初始化采样器。

        Args:
            submission_id_provider: 无参 callable，返回 submission_id 字符串；
                建议在循环开始前提供，避免 finally 阶段 ray context 已失效。
        """
        try:
            import psutil as _psutil
            self._mem_proc = _psutil.Process()
        except Exception:
            self._mem_proc = None

        self._mem_stats = RunningStats()
        self._gpu_stats = RunningStats()
        self._accum = OpStatsAccumulator()
        self._input_tracker = DeltaTracker()
        self._output_tracker = DeltaTracker()
        self._last_sample_t = _time.perf_counter()

        try:
            sid = submission_id_provider() if submission_id_provider else None
            if sid is None:
                sid = "unknown"
        except Exception:
            sid = "unknown"
        try:
            self.ctx = PerfContext.from_runtime(submission_id=sid)
        except Exception:
            self.ctx = PerfContext(session="unknown", submission_id=sid)

    def sample_realtime(self, topology) -> None:
        """实时采样：cluster + job + operator 三个维度合并执行，统一异常兜底。"""
        try:
            now_t = _time.perf_counter()
            elapsed_s = now_t - self._last_sample_t
            self._last_sample_t = now_t
            rss = self._mem_proc.memory_info().rss if self._mem_proc else 0
            sample_cluster_realtime(
                self.ctx, self._mem_stats, self._gpu_stats, rss_bytes=rss,
            )
            sample_job_realtime(
                self.ctx, topology, self._input_tracker, self._output_tracker,
            )
            sample_op_realtime(
                self.ctx, topology, self._accum, self._gpu_stats, elapsed_s,
            )
        except Exception as e:
            logger.warning("[PerfSample] sampling failed: %s", e)

    def sample_task_metrics(self, topology) -> None:
        """算子 task 耗时 & 失败计数采样。"""
        try:
            sample_op_task_metrics(self.ctx, topology, self._accum)
        except Exception as e:
            logger.warning("[PerfSample] task metrics sampling failed: %s", e)

    def flush(self, topology) -> None:
        """finally 汇总打点。"""
        try:
            flush_final(
                self.ctx, self._mem_stats, self._gpu_stats, self._accum, topology,
            )
        except Exception as e:
            logger.warning("[PerfSample] flush failed: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# 后台资源上报：cluster 级（dashboard 触发）+ job 级（driver 触发）
# ═════════════════════════════════════════════════════════════════════════════
def _report_cluster_totals_on_dashboard_init(session_name: str = ""):
    """Dashboard 初始化完成后上报集群资源（total + used），并启动定时上报线程。

    在 head.py 的 DashboardHead.run() 中调用，生命周期与集群一致。
    每次打点时动态查询最新节点资源（通过独立的 GlobalStateAccessor 直连 GCS，
    不依赖 ray.init()）。
    上报指标（集群粒度，extra2 固定为 "cluster"）：
      - cluster_cpu_total:    集群 CPU 总核数
      - cluster_gpu_total:    集群 GPU 总卡数（有 GPU 时才上报）
      - cluster_memory_total: 集群内存总量（MB）
      - cluster_cpu_used:     集群当前已占用 CPU 核数
      - cluster_gpu_used:     集群当前已占用 GPU 卡数（有 GPU 时才上报）
      - cluster_memory_used:  集群当前已占用内存（MB）
    """
    try:
        _interval_s = float(os.environ.get("RAY_PERFLOG_CLUSTER_REPORT_INTERVAL_S", "60"))
        _session_name = session_name or (
            os.environ.get("_RAY_DASHBOARD_SESSION_NAME")
            or os.environ.get("RAY_SESSION_NAME")
            or "unknown"
        )
        _gcs_address = os.environ.get("RAY_GCS_SERVER_ADDRESS") or os.environ.get("GCS_ADDRESS")

        def _query_cluster_resources():
            """通过独立的 GlobalStateAccessor 直连 GCS 动态查询集群资源（total + used），不依赖 ray.init()。"""
            import ray._raylet
            from ray._private.state import GlobalState
            from ray.core.generated import gcs_pb2
            _state = GlobalState()
            _gcs_options = ray._raylet.GcsClientOptions.create(
                _gcs_address,
                None,  # cluster_id
                allow_cluster_id_nil=True,
                fetch_cluster_id_if_nil=True,
            )
            _state._initialize_global_state(_gcs_options)
            try:
                accessor = _state._connect_and_get_accessor()
                # 查询 total
                cpu_total = 0
                gpu_total = 0.0
                mem_total_mb = 0
                obj_store_total = 0
                all_total = accessor.get_all_total_resources()
                for raw in all_total:
                    msg = gcs_pb2.TotalResources.FromString(raw)
                    for res_name, cap in msg.resources_total.items():
                        if res_name == "CPU":
                            cpu_total += int(cap)
                        elif res_name == "GPU":
                            gpu_total += cap
                        elif res_name == "memory":
                            mem_total_mb += int(cap / (1024 * 1024))
                        elif res_name == "object_store_memory":
                            obj_store_total += int(cap)
                # 查询 available
                cpu_avail = 0
                gpu_avail = 0.0
                mem_avail_mb = 0
                obj_store_avail = 0
                all_avail = accessor.get_all_available_resources()
                for raw in all_avail:
                    msg = gcs_pb2.AvailableResources.FromString(raw)
                    for res_name, cap in msg.resources_available.items():
                        if res_name == "CPU":
                            cpu_avail += int(cap)
                        elif res_name == "GPU":
                            gpu_avail += cap
                        elif res_name == "memory":
                            mem_avail_mb += int(cap / (1024 * 1024))
                        elif res_name == "object_store_memory":
                            obj_store_avail += int(cap)
                # 计算 used
                cpu_used = max(0, cpu_total - cpu_avail)
                gpu_used = max(0.0, gpu_total - gpu_avail)
                mem_used_mb = max(0, mem_total_mb - mem_avail_mb)
                obj_store_used = max(0, obj_store_total - obj_store_avail)
                return (cpu_total, gpu_total, mem_total_mb, cpu_used, gpu_used, mem_used_mb,
                        obj_store_total, obj_store_used)
            finally:
                _state.disconnect()

        def _do_report_resources():
            try:
                cpu_total, gpu_total, mem_total_mb, cpu_used, gpu_used, mem_used_mb, \
                    obj_store_total, obj_store_used = _query_cluster_resources()
                perf = RayPerfLogger.instance()
                # Total
                perf.logstash("cluster_cpu_total", _session_name, "cluster",
                              micros=0, count=cpu_total)
                if gpu_total > 0:
                    perf.logstash("cluster_gpu_total", _session_name, "cluster",
                                  micros=0, count=int(gpu_total))
                perf.logstash("cluster_memory_total", _session_name, "cluster",
                              micros=mem_total_mb * 1000, count=1)
                perf.logstash("cluster_obj_store_total", _session_name, "cluster",
                              micros=int(obj_store_total // 1024), count=1)
                # Used
                perf.logstash("cluster_cpu_used", _session_name, "cluster",
                              micros=0, count=cpu_used)
                if gpu_total > 0:
                    perf.logstash("cluster_gpu_used", _session_name, "cluster",
                                  micros=0, count=int(gpu_used))
                perf.logstash("cluster_memory_used", _session_name, "cluster",
                              micros=mem_used_mb * 1000, count=1)
                perf.logstash("cluster_obj_store_used", _session_name, "cluster",
                              micros=int(obj_store_used // 1024), count=1)
            except Exception as e:
                logger.warning("[RayPerfLogger] cluster resources report error: %s", e)

        def _resources_reporter_loop():
            while True:
                _time.sleep(_interval_s)
                _do_report_resources()

        # 立即上报一次，然后启动定时线程
        _do_report_resources()
        t = _threading.Thread(
            target=_resources_reporter_loop,
            name="RayPerfLogger-ClusterResourcesReporter",
            daemon=True,
        )
        t.start()
        logger.debug("[RayPerfLogger] cluster resources reporter thread started")

    except Exception as e:
        logger.warning("[RayPerfLogger] Failed to start cluster resources reporter: %s", e, exc_info=True)


def _report_job_resources_on_init(session_name: str = "", job_id: str = "",
                                  submission_id: str = ""):
    """ray.init() 完成后启动定时线程，每隔 60s 上报一次 job 级资源申请量。

    通过快照法查询当前 Job 内所有 ALIVE Actor 和 RUNNING Task，
    累加其 required_resources 中的 CPU/GPU，得到当前时刻该 Job 占用的资源申请量。

    注意：
    - 不在 ray.init() 时立即打点（此时 Actor/Task 尚未创建，数据为 0 无意义）。
    - 先 sleep 一个周期再开始第一次打点。
    - list_actors / list_tasks 需要 Ray 已初始化，此函数仅在 Driver 进程的
      _post_init_hooks 中调用，可以安全使用。

    Args:
        session_name: 集群 session 名，用于 extra1。
        job_id:       Ray 内部 job_id hex 字符串，用于 list_actors/list_tasks 过滤。
        submission_id: job submission_id（ray job submit 时的唯一标识），用于 extra2 打点。
                       若为空则回退到 job_id。

    上报指标（job 粒度，extra2 为 submission_id 或 job_id）：
      - job_cpu_allocated: 当前 job 内所有 ALIVE Actor + RUNNING Task 的 CPU 申请量之和
      - job_gpu_allocated: 同上，GPU 申请量之和（有 GPU 时才上报）
    """
    try:
        _interval_s = float(os.environ.get("RAY_PERFLOG_CLUSTER_REPORT_INTERVAL_S", "60"))
        _session_name = session_name or (
            os.environ.get("_RAY_DASHBOARD_SESSION_NAME")
            or os.environ.get("RAY_SESSION_NAME")
            or "unknown"
        )
        _job_id_hex = job_id          # 用于 list_actors/list_tasks 过滤
        _extra2 = submission_id or job_id  # 用于打点 extra2（优先 submission_id）

        def _query_job_resources():
            """查询当前 Job 内 ALIVE Actor + RUNNING Task 的资源申请量。"""
            from ray.util.state import list_actors, list_tasks

            cpu_total = 0.0
            gpu_total = 0.0

            # 1. 统计 ALIVE Actor 的资源申请
            try:
                alive_actors = list_actors(
                    filters=[
                        ("job_id", "=", _job_id_hex),  # 用内部 job_id 过滤
                        ("state", "=", "ALIVE"),
                    ],
                    detail=True,
                    limit=10000,
                    raise_on_missing_output=False,
                )
                for actor in alive_actors:
                    res = actor.required_resources or {}
                    cpu_total += res.get("CPU", 0.0)
                    gpu_total += res.get("GPU", 0.0)
            except Exception as e:
                logger.warning("[RayPerfLogger] list_actors error: %s", e)

            # 2. 统计 RUNNING Task 的资源申请
            # 排除 DRIVER_TASK（驱动进程本身）和 ACTOR_CREATION_TASK / ACTOR_TASK
            # （Actor 相关的 task 资源已在 list_actors 里统计过，避免重复计数）
            _excluded_types = {"DRIVER_TASK", "ACTOR_CREATION_TASK", "ACTOR_TASK"}
            try:
                running_tasks = list_tasks(
                    filters=[
                        ("job_id", "=", _job_id_hex),  # 用内部 job_id 过滤
                        ("state", "=", "RUNNING"),
                    ],
                    detail=True,
                    limit=10000,
                    raise_on_missing_output=False,
                )
                for task in running_tasks:
                    if (task.type or "") in _excluded_types:
                        continue
                    res = task.required_resources or {}
                    cpu_total += res.get("CPU", 0.0)
                    gpu_total += res.get("GPU", 0.0)
            except Exception as e:
                logger.warning("[RayPerfLogger] list_tasks error: %s", e)

            return cpu_total, gpu_total

        def _do_report_job_resources():
            try:
                cpu_total, gpu_total = _query_job_resources()
                logger.warning(
                    "[RayPerfLogger] job resources query result: cpu=%s, gpu=%s",
                    cpu_total, gpu_total,
                )
                perf = RayPerfLogger.instance()
                perf.logstash("job_cpu_allocated", _session_name, _extra2,
                              micros=0, count=int(cpu_total))
                if gpu_total > 0:
                    perf.logstash("job_gpu_allocated", _session_name, _extra2,
                                  micros=0, count=int(gpu_total))
                logger.warning("[RayPerfLogger] job resources reported successfully")
            except Exception as e:
                logger.warning("[RayPerfLogger] job resources report error: %s", e, exc_info=True)

        def _job_resources_reporter_loop():
            # 先 sleep 再打点，避免 ray.init() 时 Actor/Task 尚未创建导致数据为 0
            while True:
                _time.sleep(_interval_s)
                _do_report_job_resources()

        if not _job_id_hex:
            logger.warning("[RayPerfLogger] job_id is empty, skip job resources reporter")
            return

        t = _threading.Thread(
            target=_job_resources_reporter_loop,
            name="RayPerfLogger-JobResourcesReporter",
            daemon=True,
        )
        t.start()
        logger.warning(
            "[RayPerfLogger] job resources reporter thread started, job_id=%s, interval=%ss",
            _job_id_hex, _interval_s,
        )

    except Exception as e:
        logger.warning("[RayPerfLogger] Failed to start job resources reporter: %s", e, exc_info=True)

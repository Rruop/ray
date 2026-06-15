"""
Ray Data Perf Logger
====================
统一的 perflog 打点工厂类，管理 perf context 单例，避免重复创建相同维度的 context 对象。

使用方式：
    from ray.data._internal.execution.perf_logger import RayPerfLogger

    perf = RayPerfLogger.instance()
    perf.logstash("op_queue_cur", session_id, submission_id, op_name, micros=q_len * 1000, count=1)

实时 CPU 采样：
    # task 开始时注册
    perf.register_task(session, submission_id, op_name, worker_id)
    # task 结束时注销
    perf.unregister_task(op_name)
"""

import logging
import os
import resource
import threading
import time
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_BIZ_DEF = "ad"
_PERF_MODULE = "ray_job"
_CPU_SAMPLE_INTERVAL_S = 5.0  # 每 5 秒采样一次

# ─────────────────────────────────────────────────────────────────────────────
# 模块级 perflog warmup
#
# 必须在 import 阶段（主线程）执行，原因：
#   `infra.perflog` 内部会调用 `signal.signal()` 注册信号处理器，而
#   `signal.signal()` 仅能在主线程调用。如果延迟到第一次打点时再注册，
#   而第一次打点恰好发生在子线程（如 dashboard 的资源汇报线程、
#   subprocess module 的非主线程任务），就会抛
#   `signal only works in main thread of the main interpreter`，
#   导致整批 perf_context 创建失败、所有打点丢失。
#
# 把 enable_local + _check_pid 放到模块级，import 这个模块的进程在主线程
# 完成 signal 注册，之后任何线程的 create_perf_context 都安全。
# ─────────────────────────────────────────────────────────────────────────────
try:
    from infra.perflog import enable_local as _enable_local
    from infra.perflog.client import _PerflogClient as _PC

    _enable_local()
    _PC()._check_pid()
except Exception as _e:
    logger.warning("[RayPerfLogger] module-level warmup failed: %s", _e)


class RayPerfLogger:
    """
    Perflog 打点工厂类（单例）。

    - 按 (tag, extra1~4) 缓存 perf context 对象，避免重复创建。
    - 线程安全（driver 调度线程 + 可能的其他线程同时调用）。
    - 所有打点统一 biz_def="ad"，module="ray_job"。
    - 内置 worker 进程级全局 CPU 采样线程（每 5s 采样一次）。
    """

    _instance: Optional["RayPerfLogger"] = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def instance(cls) -> "RayPerfLogger":
        """获取全局单例（进程级别，不跨 worker 进程）。"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._cache: dict = {}
        self._cache_lock = threading.Lock()
        self._initialized = False
        self._init_pid: int = 0

        # 实时 CPU 采样相关
        # {op_name: (session, submission_id, worker_id)}
        self._active_tasks: Dict[str, Tuple[str, str, str]] = {}
        self._active_tasks_lock = threading.Lock()
        self._cpu_sampler_thread: Optional[threading.Thread] = None
        self._cpu_sampler_stop = threading.Event()
        # 上次采样时进程 CPU 时间，用于增量计算
        self._last_proc_cpu_s: float = 0.0
        self._last_sample_t: float = 0.0
        # pod 名（K8s 环境变量，固定不变）
        self._pod_name: str = (
            os.environ.get("MY_POD_NAME")
            or os.environ.get("MY_NAME")
            or os.environ.get("POD_NAME")
            or os.environ.get("HOSTNAME")
            or os.environ.get("RAY_CLOUD_INSTANCE_ID")
            or "unknown"
        )
        # service_name（extra4）：优先取 KML_MODEL_NAME，其次 KWS_SERVICE_NAME
        self._service_name: str = (
            os.environ.get("KML_MODEL_NAME")
            or os.environ.get("KWS_SERVICE_NAME")
            or ""
        )

        self._init()

    def _init(self):
        """初始化 perflog 客户端（进程级别，fork 后自动重新初始化）。"""
        current_pid = os.getpid()
        if self._initialized and self._init_pid == current_pid:
            return
        # PID 变化（fork）或首次初始化，重置状态
        self._cache = {}
        self._initialized = False
        try:
            from infra.perflog import enable_local
            from infra.perflog.client import _PerflogClient as _PC
            enable_local()
            _PC()._check_pid()
            self._initialized = True
            self._init_pid = current_pid
        except Exception as e:
            logger.warning("[RayPerfLogger] Failed to initialize perflog: %s", e)

    def _get_ctx(self, tag: str, extra1: str = "", extra2: str = "",
                 extra3: str = "", extra4: str = ""):
        """按 (tag, extra1~4) 获取或创建 perf context 单例。"""
        key = (tag, extra1, extra2, extra3, extra4)
        ctx = self._cache.get(key)
        if ctx is None:
            with self._cache_lock:
                ctx = self._cache.get(key)
                if ctx is None:
                    try:
                        from infra.perflog import create_perf_context
                        ctx = create_perf_context(
                            _PERF_MODULE, tag,
                            extra1=extra1,
                            extra2=extra2,
                            extra3=extra3,
                            extra4=extra4,
                            biz_def=_BIZ_DEF,
                        )
                        self._cache[key] = ctx
                    except Exception as e:
                        logger.warning("[RayPerfLogger] Failed to create perf ctx tag=%s: %s", tag, e)
                        return None
        return ctx

    def logstash(self, tag: str, extra1: str = "", extra2: str = "",
                 extra3: str = "", extra4: str = "",
                 micros: int = 0, count: int = 1) -> None:
        """
        打一条 perf 记录。

        Args:
            tag:      perf tag，如 "op_queue_cur"
            extra1:   通常为 session_name（集群标识）
            extra2:   通常为 submission_id（job 标识）
            extra3:   通常为 op_name（算子名）
            extra4:   自动填充 service_name（KML_MODEL_NAME 或 KWS_SERVICE_NAME），调用方无需传入
            micros:   数值指标（单位微秒，perf 展示为 Avg(ms) = micros/1000）
            count:    计数（行数、次数等）
        """
        # extra4 固定使用 service_name，忽略调用方传入值
        _extra4 = self._service_name
        try:
            ctx = self._get_ctx(tag, extra1, extra2, extra3, _extra4)
            if ctx is not None:
                ctx.logstash(micros=micros, count=count)
                ctx.persist_data()
        except Exception as e:
            logger.warning("[RayPerfLogger] logstash failed tag=%s: %s", tag, e)

    # ------------------------------------------------------------------ #
    # 实时 CPU 采样                                                         #
    # ------------------------------------------------------------------ #

    def register_task(self, session: str, submission_id: str,
                      op_name: str, worker_id: str) -> None:
        """注册一个正在运行的 task，启动采样线程（如未启动）。"""
        with self._active_tasks_lock:
            self._active_tasks[op_name] = (session, submission_id, worker_id)
        self._ensure_sampler_running()

    def unregister_task(self, op_name: str) -> None:
        """task 结束时注销，无 active task 时停止采样线程。"""
        with self._active_tasks_lock:
            self._active_tasks.pop(op_name, None)
            if not self._active_tasks:
                self._cpu_sampler_stop.set()

    def _ensure_sampler_running(self) -> None:
        """确保采样线程已启动（进程级别只有一个）。"""
        if (self._cpu_sampler_thread is not None
                and self._cpu_sampler_thread.is_alive()):
            return
        self._cpu_sampler_stop.clear()
        self._last_proc_cpu_s = self._get_proc_cpu_s()
        self._last_sample_t = time.monotonic()
        t = threading.Thread(target=self._cpu_sampler_loop, daemon=True)
        t.start()
        self._cpu_sampler_thread = t

    def _get_proc_cpu_s(self) -> float:
        """获取当前进程累计 CPU 时间（user + sys，单位秒）。"""
        try:
            ru = resource.getrusage(resource.RUSAGE_SELF)
            return ru.ru_utime + ru.ru_stime
        except Exception:
            return self._last_proc_cpu_s

    def _cpu_sampler_loop(self) -> None:
        """后台采样线程主循环：每 5s 采样一次进程 CPU 利用率，同时打 worker 和算子粒度。"""
        import os as _os
        _cpu_count = _os.cpu_count() or 1  # 本机物理核数，用于 cap
        while not self._cpu_sampler_stop.wait(timeout=_CPU_SAMPLE_INTERVAL_S):
            try:
                _now_t = time.monotonic()
                _now_cpu_s = self._get_proc_cpu_s()
                _elapsed = _now_t - self._last_sample_t
                _cpu_delta = _now_cpu_s - self._last_proc_cpu_s
                self._last_sample_t = _now_t
                self._last_proc_cpu_s = _now_cpu_s

                if _elapsed <= 0:
                    continue
                _cores = min(_cpu_delta / _elapsed, _cpu_count)  # cap 到物理核数

                with self._active_tasks_lock:
                    tasks_snapshot = dict(self._active_tasks)

                if not tasks_snapshot:
                    continue

                # 取第一个 task 的 session/submission_id/worker_id（同一 worker 进程这些是固定的）
                _first = next(iter(tasks_snapshot.values()))
                _session, _submission_id, _worker_id = _first

                # worker 粒度：整个进程的 CPU 利用率
                self.logstash(
                    "worker_cpu_rt", _session, _submission_id, _worker_id,
                    micros=int(_cores * 1000), count=1,
                )

                # 算子粒度：进程 CPU 均摊到各 active 算子，避免重复计数
                _num_ops = len(tasks_snapshot)
                _cores_per_op = _cores / _num_ops if _num_ops > 0 else _cores
                for _op_name, (_s, _sid, _wid) in tasks_snapshot.items():
                    self.logstash(
                        "op_cpu_rt", _s, _sid, _op_name,
                        micros=int(_cores_per_op * 1000), count=1,
                    )

                # GPU 利用率采样（worker 粒度、算子粒度、pod 粒度）
                _gpu_utils = self._get_gpu_utilization()
                if _gpu_utils:
                    _avg_util = sum(_gpu_utils) / len(_gpu_utils)
                    _gpu_count = len(_gpu_utils)
                    _used_cards = _gpu_count * _avg_util / 100.0  # 等效使用卡数
                    # worker 粒度：本 worker 进程可见 GPU 的平均利用率
                    self.logstash(
                        "worker_gpu_util", _session, _submission_id, _worker_id,
                        micros=int(_avg_util * 10), count=1,  # Avg(ms)=util%/100
                    )
                    # pod 粒度：同一 pod 内所有 GPU 的平均利用率
                    self.logstash(
                        "pod_gpu_util", _session, _submission_id, self._pod_name,
                        micros=int(_avg_util * 10), count=1,
                    )
                    # pod 粒度：GPU 实际使用卡数（等效卡数 = GPU数 × 平均利用率）
                    self.logstash(
                        "pod_gpu_used_cards", _session, _submission_id, self._pod_name,
                        micros=int(_used_cards * 1000), count=1,  # Avg(ms) = used_cards
                    )
                    # 算子粒度
                    for _op_name, (_s, _sid, _wid) in tasks_snapshot.items():
                        self.logstash(
                            "op_gpu_util", _s, _sid, _op_name,
                            micros=int(_avg_util * 10), count=1,
                        )
            except Exception as e:
                logger.warning("[RayPerfLogger] CPU sampler error: %s", e)

    def _get_gpu_utilization(self):
        """获取当前进程可见 GPU 的利用率列表（0~100），失败返回空列表。"""
        try:
            import pynvml
            pynvml.nvmlInit()
            count = pynvml.nvmlDeviceGetCount()
            utils = []
            for i in range(count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                utils.append(util.gpu)  # 0~100 整数
            return utils
        except Exception:
            pass
        # fallback: nvidia-smi
        try:
            import subprocess
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu",
                 "--format=csv,noheader,nounits"],
                timeout=2,
            ).decode().strip()
            return [float(x) for x in out.splitlines() if x.strip()]
        except Exception:
            return []


# 集群级 / Job 级资源后台上报函数已迁移到 perf_metrics.py：
#   - _report_cluster_totals_on_dashboard_init
#   - _report_job_resources_on_init
# 请通过 `from ray.data._internal.execution import perf_metrics` 调用。




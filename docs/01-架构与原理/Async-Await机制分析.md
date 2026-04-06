# Python async/await 机制与 Ray Dashboard 优化详解

## 1. 问题背景

Ray Dashboard 的 `/logical/actors` 接口在大规模集群下返回缓慢。优化过程中将 `_get_actor_info` 从 `async def` 改为 `def`，并通过 `loop.run_in_executor` 将计算 offload 到线程池。本文档详细分析 async/await 的底层机制以及优化的原理。

---

## 2. `async def` / `await` / coroutine 基础

### 2.1 三个阶段

#### 阶段一：`async def` — 定义（不执行）

```python
async def foo():
    print("hello")
    return 1
```

这只是定义了一个协程函数，和普通 `def` 一样，什么都不会执行。

#### 阶段二：调用协程函数 — 创建 coroutine（不执行）

```python
coro = foo()   # 创建 coroutine 对象，不执行任何代码！
```

调用 `foo()` 不会打印 "hello"。它只返回一个 `<coroutine object>`，相当于一个"待执行的任务包装"。

#### 阶段三：`await` — 驱动执行

```python
result = await coro   # 这里才真正执行 foo() 内部代码
```

`await` 做了两件事：
1. **驱动 coroutine 执行**
2. **如果 coroutine 内部遇到 `await`，把控制权还给事件循环**

### 2.2 完整执行流程示例

```python
async def foo():          # ① 定义：什么都不做
    result = await bar()  # ④ 执行到 await bar()
    return result         # ⑧ bar() 完成后，恢复执行，返回结果

async def bar():          # ① 定义：什么都不做
    await asyncio.sleep(1)# ⑤ 遇到 IO，挂起 foo，交出控制权
    return 42             # ⑦ sleep 完成，恢复 bar，返回 42

coro = foo()              # ② 创建 coroutine，不执行
result = await coro       # ③ 开始执行 foo()
                          # ④ foo 内部 await bar() → 执行 bar()
                          # ⑤ bar 内部 await sleep → 挂起，控制权还给事件循环
                          # ⑥ 1秒后事件循环恢复 bar()
                          # ⑦ bar 返回 42
                          # ⑧ foo 得到 result=42，继续执行
```

### 2.3 可以被 `await` 的类型

| 类型 | 来源 | 举例 |
|------|------|------|
| **coroutine** | 调用 `async def` 函数 | `await _get_actor_info(entry)` |
| **Future** | `run_in_executor()` 等返回 | `await loop.run_in_executor(None, func)` |
| **Task** | `asyncio.create_task()` | `await asyncio.create_task(coro)` |
| **awaitable 对象** | 实现了 `__await__` 方法 | 自定义类 |

判断方式：

```python
import asyncio

# 调用 async def → 返回 coroutine
async def foo():
    return 1
coro = foo()
print(type(coro))              # <class 'coroutine'>
print(asyncio.iscoroutine(coro))  # True

# run_in_executor → 返回 Future
loop = asyncio.get_event_loop()
fut = loop.run_in_executor(None, func, args)
print(type(fut))               # <class 'asyncio.Future'>
print(asyncio.isfuture(fut))   # True

# create_task → 返回 Task（Future 的子类）
task = asyncio.create_task(foo())
print(type(task))              # <class 'asyncio.Task'>
print(isinstance(task, asyncio.Future))  # True

# 普通 int → 不能 await
# await 1  → TypeError: object int can't be used in 'await' expression
```

---

## 3. `await` 让出控制权的机制

### 3.1 核心规则

**`await` 是否让出控制权，取决于被 await 的函数内部有没有 `await`（即有没有 IO 点）。**

```python
# 不让出 — 内部没有 await
async def compute():
    result = heavy_calc()   # 没有 await
    return result

await compute()  # compute 跑完才回来，中间不让出

# 让出 — 内部有 await
async def fetch():
    data = await http_get()  # 有 await（IO）
    return data

await fetch()  # fetch 遇到 IO 挂起，事件循环可以处理别的
```

### 3.2 对比三种 `await` 的行为

```python
# 1. await 一个"内部没有 await"的 coroutine → 不让出
result = await _get_actor_info(entry)

# 2. await 一个 Future（run_in_executor 返回的）→ 让出
result = await loop.run_in_executor(None, func, args)

# 3. await 一个有 IO 的 coroutine → 让出
result = await http_get(url)
```

区别在于 **`await` 后面的对象能不能立即给出结果**：

| await 后面的对象 | 能立即给结果？ | 行为 |
|---|---|---|
| `_get_actor_info(entry)` (coroutine) | 能，内部纯 CPU | 事件循环直接执行它，跑完返回 |
| `run_in_executor(...)` (Future) | 不能，线程池还在跑 | 事件循环挂起当前协程，去处理别的 |
| `http_get(url)` (coroutine) | 不能，网络 IO 中 | 事件循环挂起当前协程，去处理别的 |

### 3.3 `await` 不等于"让出"

```
await 的行为取决于后面的对象：

对象能立即给结果（纯 CPU coroutine）
  → 事件循环直接执行，跑完返回
  → 不让出，其他请求排队

对象暂时给不出结果（Future / IO coroutine）
  → 事件循环挂起当前协程
  → 去处理其他请求
  → 结果好了再恢复
```

**`await` 不是"让出控制权"的语法，而是"等待结果"的语法。让出控制权要靠被 await 的函数内部主动 `await` 别的 IO 操作。**

### 3.4 如果不 `await` 会怎样

```python
async def handler():
    response = http_get(url)   # 不 await，只是创建 coroutine，不执行！
    return response            # 返回一个 coroutine 对象，不是数据
```

不 `await` 的话：
- `http_get(url)` 只创建 coroutine，**根本不会发起网络请求**
- 返回的是 `<coroutine object>`，不是数据
- Python 会报 `RuntimeWarning: coroutine was never awaited`

所以 `http_get()` **必须** `await`，不是"在线程里执行"的问题，是**不 await 就根本不执行**。

---

## 4. `await` 底层实现：`yield` 机制

### 4.1 `await` 在底层就是 `yield`

Python 编译器把 `await` 直接翻译成 `yield`：

```python
# 你写的
async def http_get(url):
    ...
    return await future

# Python 编译后等价于
async def http_get(url):
    ...
    result = yield future    # await → yield
    return result
```

可以验证：

```python
import asyncio

async def foo():
    return await asyncio.sleep(1)

# foo 是一个生成器函数
print(type(foo()))          # <class 'coroutine'>
print(hasattr(foo(), 'send'))  # True，和生成器一样有 send 方法
```

**coroutine 本质上就是一个生成器**，`await` 就是 `yield`。

### 4.2 纯 CPU coroutine — 没有 yield

```python
# 纯 CPU coroutine
async def _get_actor_info(actor):
    actor = actor.copy()      # CPU
    actor.update(stats)       # CPU
    return actor              # 直接 return，没有 yield

# 等价于生成器：
def _get_actor_info(actor):
    actor = actor.copy()
    actor.update(stats)
    return result           # 直接 return，没有 yield
                           # 事件循环 next() 一次就 StopIteration 了
```

### 4.3 有 IO 的 coroutine — 会 yield

```python
async def http_get(url):
    # 创建 Future，IO 还没完成
    future = loop.create_future()
    
    # 注册 IO 回调：IO 完成后调用 future.set_result(data)
    fd = socket.connect(url)
    loop.add_reader(fd, lambda: future.set_result(read_data(fd)))
    
    # 关键：await future → 底层 yield future 给事件循环
    return await future

# 等价于生成器：
def http_get(url):
    future = loop.create_future()
    fd = socket.connect(url)
    loop.add_reader(fd, lambda: future.set_result(read_data(fd)))
    result = yield future   # yield future 给事件循环，暂停
    return result           # 事件循环 send(result) 恢复后返回
```

### 4.4 事件循环的核心判断逻辑

```python
# 事件循环驱动 coroutine（简化版）
def _run_until_complete(self, coro):
    # 驱动 coroutine 执行
    try:
        result = coro.send(None)    # 恢复 coroutine
    except StopIteration:
        return                      # coroutine 执行完毕
    
    # result 是 coroutine yield 出来的东西
    if isinstance(result, Future):
        if result.done():           # ← 核心判断
            # 结果已经好了，立即恢复 coroutine
            coro.send(result.result())
        else:
            # 结果还没好，注册回调，去处理其他事
            result.add_done_callback(lambda f: coro.send(f.result()))
            self._process_other_tasks()  # 处理其他请求
```

**Python 判断能不能立即给结果，就是看 yield 出来的 `Future.done()` 是 True 还是 False。纯 CPU coroutine 不会 yield，所以根本没有检查机会，一路跑完。**

### 4.5 `run_in_executor` 为什么能让出

```python
def run_in_executor(self, executor, func, *args):
    future = self.create_future()  # 创建 Future（done=False）
    
    def _callback():
        try:
            result = func(*args)           # 线程池中执行
            future.set_result(result)      # 设置结果，done=True
        except Exception as e:
            future.set_exception(e)
    
    executor.submit(_callback)  # 提交到线程池
    return future               # 返回 Future
```

执行过程：

```
await loop.run_in_executor(None, func, args)
  → run_in_executor 把 func 提交到 ThreadPoolExecutor
  → 返回 Future（done=False，线程池还没跑完）
  → await future → yield future 给事件循环
  → future.done() == False
  → 事件循环挂起当前协程，去处理其他请求  ← 让出了！
  
  ... 线程池跑完 ...
  → future.set_result(result)
  → future.done() == True
  → 事件循环恢复协程
```

### 4.6 完整流程对比图

```
await something 的底层执行：

something.__await__()
  → 生成器开始执行
  
  情况A：纯 CPU，一路跑完
    → 没有 yield
    → StopIteration(result)
    → 立即得到结果，不让出

  情况B：遇到 IO / Future
    → yield future（done=False）
    → 事件循环检查 future.done() == False
    → 挂起当前协程，处理其他请求
    → IO 完成 / 线程池完成 → future.set_result()
    → future.done() == True
    → 恢复协程
```

---

## 5. Ray Dashboard `/logical/actors` 优化分析

### 5.1 旧代码的问题

```python
# 旧版 _get_actor_info 是 async def，但内部没有任何 await
async def _get_actor_info(actor):
    actor = actor.copy()            # CPU
    actor.update(stats)             # CPU
    return actor                    # 没有 await → 不让出控制权

# 旧版 get_actor_infos 串行 await 每一个
async def get_actor_infos(cls, actor_ids=None):
    return {
        actor_id: await _get_actor_info(entry)
        for actor_id, entry in entries.items()
    }
```

执行过程：

```
事件循环线程:
  await _get_actor_info(entry1)  → 创建coroutine → 执行 → 跑完（不让出） → 返回
  await _get_actor_info(entry2)  → 创建coroutine → 执行 → 跑完（不让出） → 返回
  await _get_actor_info(entry3)  → 创建coroutine → 执行 → 跑完（不让出） → 返回
  ... 10万次
```

**整段时间事件循环被占满，其他 HTTP 请求全部排队等待。**

旧代码的 `await _get_actor_info(entry)` 确实执行了，但因为函数内部没有 `await`，它从头到尾占着事件循环不放，和同步代码效果一样，还多了 coroutine 创建开销。

### 5.2 为什么 `await _get_actor_info(entry)` 不让出

`_get_actor_info` 内部没有 `await`，等于纯 CPU 生成器没有 `yield`，事件循环 `send()` 一次就 `StopIteration` 了，中间没有任何机会切出去处理其他请求。

```
await _get_actor_info(entry1)  → 立即执行完，不让出
await _get_actor_info(entry2)  → 立即执行完，不让出
await _get_actor_info(entry3)  → 立即执行完，不让出
... 10万次，事件循环一直被占着
```

### 5.3 新代码的优化

```python
# _get_actor_info 改为普通同步函数
def _get_actor_info(actor):         # 去掉 async
    actor = actor.copy()
    actor.update(stats)
    return actor

# 批量同步方法
def _get_actor_infos_sync(cls, entries):
    return {actor_id: cls._get_actor_info(entry) for actor_id, entry in entries.items()}

# async 入口，用 run_in_executor 丢到线程池
async def get_actor_infos(cls, actor_ids=None):
    ...
    loop = get_or_create_event_loop()
    return await loop.run_in_executor(
        None,
        cls._get_actor_infos_sync,
        target_actor_table_entries,
    )
```

执行过程：

```
事件循环线程:                    线程池:
  await run_in_executor(...)  →   _get_actor_infos_sync(entries) 开始执行
  挂起，交出控制权 ←               处理10万个actor...
  处理其他HTTP请求                 ...
  处理其他HTTP请求                 ...
  线程池完成 → 恢复协程          ← 返回结果
  拿到结果，返回
```

### 5.4 `await loop.run_in_executor(...)` 为什么生效

```python
await loop.run_in_executor(None, cls._get_actor_infos_sync, entries)
```

1. `run_in_executor()` 把 `_get_actor_infos_sync` 提交到 `ThreadPoolExecutor`
2. 返回一个 `asyncio.Future` 对象（`done=False`，线程池还没跑完）
3. `await` 这个 Future → `yield` future 给事件循环
4. 事件循环检查 `future.done() == False` → 挂起当前协程
5. 事件循环去处理其他 HTTP 请求
6. 线程池执行完毕 → `future.set_result(result)`
7. 事件循环恢复协程

**关键：`await` 挂起的是 `get_actor_infos` 这个协程，不是整个事件循环。** 事件循环在等线程池结果的期间，可以处理其他请求。

### 5.5 三个层面的优化

| 层面 | 旧代码 | 新代码 | 为什么快 |
|------|--------|--------|----------|
| **去掉假 async** | `async def` + 创建 coroutine + 事件循环调度 | 普通 `def`，直接调用 | 省去 10 万次 coroutine 创建和调度开销 |
| **不阻塞事件循环** | 串行 await 占满事件循环 | `run_in_executor` 丢到线程池 | 其他 HTTP 请求可以正常处理 |
| **批量处理** | 10 万次逐个 await | 1 次线程池调用，内部批量处理 | 减少跨线程通信次数 |

---

## 6. 请求耗时与并发性

### 6.1 该请求自身的返回时间

**`run_in_executor` 没有缩短该请求自身的返回时间。**

```
旧代码：事件循环线程 → 串行计算 10 万个 actor → 10s
新代码：线程池线程   → 串行计算 10 万个 actor → ~10s（还多了线程提交和唤醒开销，可能略慢）
```

`_get_actor_info` 是纯 CPU 计算，不管在哪里跑，10 万个 Actor 的计算量不变。`run_in_executor` 不是让计算变快，是**把计算搬离事件循环线程**。

### 6.2 真正受益的是其他请求

```
时间轴：

旧代码：
  事件循环线程: [========== /logical/actors 计算 10s ==========]
  用户A /api/jobs:        ........................等待10s.......................响应
  用户B /api/nodes:       ........................等待10s.......................响应

新代码：
  事件循环线程: [提交线程池][处理A请求][处理B请求][...][拿结果返回]
  线程池:      [========== /logical/actors 计算 10s ==========]
  用户A /api/jobs:   请求 → 立即响应
  用户B /api/nodes:  请求 → 立即响应
```

### 6.3 真正缩短自身耗时的是 pid 索引优化

```
旧：每个 actor 线性扫描 workers 找 pid → O(actors × workers_per_node)
新：pid 哈希索引直接查 → O(actors)

10万 actor × 1000 workers/node → 1亿次比较 vs 10万次哈希查找
```

**总结：`run_in_executor` 优化的是并发性（其他请求不被阻塞），pid 索引优化的是自身耗时。**
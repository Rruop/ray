# Boost.Asio 与 io_context 完整技术文档

本文档系统整理 Boost.Asio 框架中 io_context 的核心概念、内部数据结构、代码逻辑与调用链路，涵盖从用户层 API 到操作系统内核的完整路径。

---

## 目录

- [一、io_context 是什么](#一io_context-是什么)
- [二、io_context 核心职责与架构分层](#二io_context-核心职责与架构分层)
- [三、io_context 基本工作模型与代码示例](#三io_context-基本工作模型与代码示例)
- [四、io_context 内部结构与组件关系](#四io_context-内部结构与组件关系)
- [五、核心数据结构设计](#五核心数据结构设计)
  - [5.1 operation — 所有任务的基类](#51-operation--所有任务的基类)
  - [5.2 op_queue — 侵入式任务队列](#52-op_queue--侵入式任务队列)
  - [5.3 scheduler — 调度器核心](#53-scheduler--调度器核心)
  - [5.4 completion_handler — 用户任务包装](#54-completion_handler--用户任务包装)
  - [5.5 call_stack — 线程局部调用栈](#55-call_stack--线程局部调用栈)
- [六、run() 内部逻辑与完整执行流程](#六run-内部逻辑与完整执行流程)
  - [6.1 run() 源码级流程分析](#61-run-源码级流程分析)
  - [6.2 do_run_one() 单步执行详解](#62-do_run_one-单步执行详解)
  - [6.3 run() 退出条件完整分析](#63-run-退出条件完整分析)
- [七、异步 I/O 完整调用链路](#七异步-io-完整调用链路)
  - [7.1 从 async_read 到 handler 执行的完整路径](#71-从-async_read-到-handler-执行的完整路径)
  - [7.2 Reactor 事件通知机制](#72-reactor-事件通知机制)
  - [7.3 定时器异步等待链路](#73-定时器异步等待链路)
- [八、dispatch 与 post 深度分析](#八dispatch-与-post-深度分析)
  - [8.1 核心区别对比](#81-核心区别对比)
  - [8.2 dispatch 源码逻辑与调用链路](#82-dispatch-源码逻辑与调用链路)
  - [8.3 post 源码逻辑与调用链路](#83-post-源码逻辑与调用链路)
  - [8.4 can_dispatch 关键澄清](#84-can_dispatch-关键澄清)
  - [8.5 四种调用场景与调用栈追踪](#85-四种调用场景与调用栈追踪)
  - [8.6 is_continuation 标志与调度优化](#86-is_continuation-标志与调度优化)
- [九、多线程使用模型](#九多线程使用模型)
  - [9.1 线程池模式](#91-线程池模式)
  - [9.2 线程安全机制分析](#92-线程安全机制分析)
  - [9.3 strand 串行化保证](#93-strand-串行化保证)
- [十、work_guard 防止提前退出](#十work_guard-防止提前退出)
- [十一、设计模式总结](#十一设计模式总结)
- [十二、使用建议与陷阱](#十二使用建议与陷阱)
- [附录：核心概念速查](#附录核心概念速查)

---

## 一、io_context 是什么

`io_context`（旧版本叫 `io_service`）是 Boost.Asio 的核心类，它是整个异步 I/O 框架的调度中枢。

```
io_context = 任务队列 + 事件循环 + I/O 多路复用器
```

一句话定义：**io_context 是连接你的程序和操作系统 I/O 服务的桥梁，负责调度和执行所有异步操作的完成处理函数（handler）。**

通俗类比：

| 概念 | 类比 |
|------|------|
| post/dispatch | 顾客点单（任务进入队列） |
| `run()` | 服务员开始工作，按单出餐 |
| 多线程 `run()` | 多个服务员一起干活 |
| epoll/IOCP | 后厨通知系统（菜好了就喊服务员来端） |
| work_guard | 告诉服务员"别下班，还会有客人来" |

---

## 二、io_context 核心职责与架构分层

### 2.1 三大核心功能

```
┌──────────────────────────────────────────┐
│              io_context                    │
├──────────────────────────────────────────┤
│  1. 任务队列管理                            │
│     - 存储待执行的 handler                  │
│     - post / dispatch 把任务放进来           │
│                                            │
│  2. 事件循环 (run)                          │
│     - 不断从队列取出任务执行                  │
│     - 阻塞等待 I/O 事件                      │
│                                            │
│  3. I/O 多路复用 (与操作系统交互)             │
│     - Linux: epoll                         │
│     - Windows: IOCP                        │
│     - macOS/BSD: kqueue                    │
└──────────────────────────────────────────┘
```

### 2.2 整体分层架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      用户层 (User Layer)                       │
│   socket / timer / async_read / async_write / co_spawn       │
├─────────────────────────────────────────────────────────────┤
│                    前端接口层 (Frontend)                       │
│        io_context / executor / dispatch / post               │
├─────────────────────────────────────────────────────────────┤
│                    调度层 (Scheduler)                          │
│     scheduler / op_queue / operation / completion_handler    │
├─────────────────────────────────────────────────────────────┤
│                  反应器层 (Reactor) - 平台相关                  │
│      epoll_reactor / iocp / kqueue_reactor / select          │
├─────────────────────────────────────────────────────────────┤
│                  操作系统层 (OS Layer)                         │
│         epoll() / IOCP / kqueue() / select() 系统调用          │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 层间调用关系

```
用户层 API (async_read, post, dispatch)
    │
    ▼ 封装为 operation 对象
前端接口层 (io_context::executor_type)
    │
    ▼ 调用 scheduler 方法
调度层 (scheduler::post_immediate_completion, scheduler::run)
    │
    ▼ 注册/等待 fd 事件
反应器层 (epoll_reactor::register_descriptor, epoll_reactor::wait)
    │
    ▼ 系统调用
操作系统层 (epoll_ctl, epoll_wait)
```

---

## 三、io_context 基本工作模型与代码示例

### 3.1 最简单的例子 — post 任务

```cpp
#include <boost/asio.hpp>
#include <iostream>

int main()
{
    boost::asio::io_context ioc;

    boost::asio::post(ioc, []{
        std::cout << "Hello from io_context!\n";
    });

    ioc.run();

    std::cout << "All tasks done\n";
    return 0;
}
```

输出：

```
Hello from io_context!
All tasks done
```

### 3.2 执行流程图解

```
   主线程
     │
     ├─ 创建 ioc
     │
     ├─ post(任务A)  ──────> [队列: A]
     │
     ├─ ioc.run() ─┐
     │             │  事件循环开始
     │             ▼
     │      ┌─────────────────┐
     │      │ 队列空了吗?       │
     │      └────┬────────────┘
     │       否  │
     │           ▼
     │      取出任务A并执行  → 打印 "Hello..."
     │           │
     │           ▼
     │      ┌─────────────────┐
     │      │ 队列空了吗?       │ ──是──> run() 返回
     │      └─────────────────┘
     │
     └─ 打印 "All tasks done"
```

### 3.3 异步定时器示例

```cpp
#include <boost/asio.hpp>
#include <iostream>

int main()
{
    boost::asio::io_context ioc;

    boost::asio::steady_timer timer(ioc, std::chrono::seconds(2));

    timer.async_wait([](const boost::system::error_code& ec){
        std::cout << "Timer expired!\n";
    });

    std::cout << "Waiting...\n";
    ioc.run();

    return 0;
}
```

关键点：
- `async_wait` 立即返回，不阻塞
- `ioc.run()` 内部调用 `epoll_wait`（Linux）等待定时器事件
- 2 秒后，操作系统通知 io_context，取出回调并执行

---

## 四、io_context 内部结构与组件关系

### 4.1 顶层结构

```cpp
class io_context : public execution_context
{
private:
    impl_type& impl_;
};
```

`impl_type` 根据平台不同：
- Linux: `scheduler` + `epoll_reactor`
- Windows: `win_iocp_io_context`
- macOS/BSD: `scheduler` + `kqueue_reactor`

### 4.2 内部组件关系总览

```
        io_context
            │ 拥有
            ▼
        scheduler ◄──────── 实现 ───────── execution_context (基类)
            │
            ├── op_queue_<operation>   任务队列
            ├── reactor (平台相关)       I/O 多路复用
            ├── mutex_                  线程同步
            ├── outstanding_work_       工作计数
            ├── wakeup_event_           唤醒机制
            └── thread_call_stack       线程上下文追踪
                    │
                    ▼
              operation (基类)
                    │
        ┌───────────┼────────────┐
        ▼           ▼            ▼
completion_handler  reactor_op  descriptor_state
   (普通任务)      (I/O 任务)    (fd 状态)
```

### 4.3 scheduler 与 reactor 的交互

```
┌────────────────────────────────────────────┐
│              scheduler                      │
│                                            │
│  op_queue_ ←── reactor 通过此队列传递事件  │
│                                            │
│  run() 循环中：                              │
│    1. 处理 op_queue_ 中的任务               │
│    2. 若队列空且无 task_interrupted_，       │
│       调用 reactor->wait() 阻塞等待         │
│    3. reactor 返回后，将完成事件             │
│       push 到 op_queue_                     │
│    4. 继续处理队列                          │
└────────────────────────────────────────────┘
         │                         │
         │ post_immediate_completion│
         │ (用户投递)               │
         ▼                         │
┌────────────────────────────────────────────┐
│              reactor (epoll_reactor)        │
│                                            │
│  1. register_descriptor(): 注册 fd 到 epoll │
│  2. enable_operation(): 设置监控的事件类型   │
│  3. wait(): 调用 epoll_wait 系统调用       │
│  4. 返回活跃 fd 列表，对每个 fd：            │
│     - 取出关联的 descriptor_state           │
│     - 将就绪的 reactor_op push 到           │
│       scheduler.op_queue_                   │
│     - 通过 wakeup 唤醒 run() 线程          │
└────────────────────────────────────────────┘
```

---

## 五、核心数据结构设计

### 5.1 operation — 所有任务的基类

这是整个框架最核心的数据结构，所有异步任务都是 `operation` 的派生类。

```cpp
class scheduler_operation
{
public:
    typedef void (*func_type)(
        void* owner,
        scheduler_operation* base,
        const boost::system::error_code& ec,
        std::size_t bytes_transferred);

    void complete(void* owner,
        const boost::system::error_code& ec,
        std::size_t bytes_transferred)
    {
        func_(owner, this, ec, bytes_transferred);
    }

    void destroy()
    {
        func_(0, this, boost::system::error_code(), 0);
    }

protected:
    scheduler_operation(func_type func)
        : next_(0),
          func_(func),
          task_result_(0)
    {}

private:
    friend class op_queue_access;

    scheduler_operation* next_;
    func_type func_;
    std::size_t task_result_;
};
```

**内存布局：**

```
operation 的内存布局
┌──────────────────────────┐
│  next_  (8 bytes)         │ ← 侵入式链表指针
├──────────────────────────┤
│  func_  (8 bytes)         │ ← 函数指针(代替vtable)
├──────────────────────────┤
│  task_result_ (8 bytes)   │ ← 结果存储
├──────────────────────────┤
│  派生类数据...             │ ← handler、buffer等
└──────────────────────────┘
```

**关键设计决策：为什么用函数指针而不是虚函数？**

| 设计选择 | 函数指针 | 虚函数 |
|----------|----------|--------|
| 内存开销 | 无 vtable 指针（省 8 bytes） | 每个 object 有 vptr |
| 缓存友好 | 函数指针紧邻数据，局部性好 | vtable 在独立内存区域 |
| 执行/销毁统一 | `owner != 0` 执行，`owner == 0` 销毁，同一入口 | 需 separate `execute()`/`destroy()` |
| 派生类灵活性 | 每个派生类注册自己的静态函数 | 虚函数表固定 |

**调用链路：**

```
scheduler::do_run_one()
    │
    ├─ op = op_queue_.front(); op_queue_.pop();
    │
    ├─ lock.unlock();
    │
    ▼
op->complete(owner, ec, bytes_transferred)
    │
    ▼ 调用 func_ 函数指针
completion_handler::do_complete(owner, base, ec, bytes_transferred)
    │
    ├─ static_cast<completion_handler*>(base)  ← 还原派生类
    │
    ├─ 取出 handler
    │
    ├─ p.reset()  ← 释放 operation 内存
    │
    ▼
invoke(handler)  ← 执行用户回调
```

### 5.2 op_queue — 侵入式任务队列

```cpp
template <typename Operation>
class op_queue
{
public:
    op_queue() : front_(0), back_(0) {}

    ~op_queue()
    {
        while (Operation* op = front_)
        {
            pop();
            op->destroy();
        }
    }

    Operation* front() { return front_; }

    void pop()
    {
        if (front_)
        {
            Operation* tmp = front_;
            front_ = op_queue_access::next(front_);
            if (front_ == 0) back_ = 0;
            op_queue_access::next(tmp, static_cast<Operation*>(0));
        }
    }

    void push(Operation* h)
    {
        op_queue_access::next(h, static_cast<Operation*>(0));
        if (back_)
        {
            op_queue_access::next(back_, h);
            back_ = h;
        }
        else
        {
            front_ = back_ = h;
        }
    }

    void push(op_queue<Operation>& q)
    {
        if (Operation* other_front = q.front_)
        {
            if (back_)
                op_queue_access::next(back_, other_front);
            else
                front_ = other_front;
            back_ = q.back_;
            q.front_ = 0;
            q.back_ = 0;
        }
    }

    bool empty() const { return front_ == 0; }

private:
    Operation* front_;
    Operation* back_;
};
```

**队列结构图：**

```
op_queue
  front_ ──┐                              ┌── back_
           ▼                              ▼
     ┌──────────┐    ┌──────────┐    ┌──────────┐
     │ op1      │───>│ op2      │───>│ op3      │──> nullptr
     │ next_ ───┼─┐  │ next_    │    │ next_    │
     └──────────┘ │  └──────────┘    └──────────┘
                  └─────►
```

**设计特点：**

| 特性 | 说明 |
|------|------|
| 侵入式 | `next_` 指针直接在 operation 内部，无需额外内存分配 |
| O(1) 入队 | `push()` 只操作 `front_`/`back_` 指针 |
| O(1) 出队 | `pop()` 只移动 `front_` 指针 |
| O(1) 合并 | `push(q)` 两个队列拼接，整批移动 |
| RAII 析构 | ~op_queue 自动 destroy 所有未执行 operation |

### 5.3 scheduler — 调度器核心

```cpp
class scheduler : public execution_context_service_base<scheduler>
{
public:
    std::size_t run(boost::system::error_code& ec);
    std::size_t run_one(boost::system::error_code& ec);
    void post_immediate_completion(operation* op, bool is_continuation);

private:
    mutable mutex mutex_;
    op_queue<operation> op_queue_;
    reactor* task_;
    struct task_operation : operation
    {
        task_operation() : operation(0) {}
    } task_operation_;
    bool task_interrupted_;
    atomic_count outstanding_work_;
    bool stopped_;
    bool shutdown_;
    const bool one_thread_;
    event wakeup_event_;
};
```

**scheduler 内存结构：**

```
┌─────────────────────────────────────────┐
│            scheduler                      │
├─────────────────────────────────────────┤
│  mutex_                  线程同步锁        │
│  op_queue_              ┌──────────────┐  │
│                        │ op1→op2→op3  │  │
│                        └──────────────┘  │
│  task_ ──────────────► epoll_reactor     │
│  task_operation_        标记节点          │
│  outstanding_work_ = 5  未完成工作=5      │
│  stopped_ = false                        │
│  one_thread_ = false                     │
│  wakeup_event_          唤醒信号          │
└─────────────────────────────────────────┘
```

**scheduler 关键成员作用详解：**

| 成员 | 作用 | 调用链路影响 |
|------|------|-------------|
| `mutex_` | 保护 `op_queue_` 和状态变量 | `run()` 加锁取任务，`post_immediate_completion()` 加锁入队 |
| `op_queue_` | 存储 operation 对象 | `run()` → `do_run_one()` 从此取任务执行 |
| `task_` | 平台相关 reactor 指针 | `run()` 空队列时调用 `task_->run()` → `epoll_wait` |
| `task_operation_` | reactor 的占位 operation | 当 reactor 需要被处理时，此节点在 `op_queue_` 中标记 |
| `task_interrupted_` | reactor 是否已入队 | 避免重复将 `task_operation_` 入队 |
| `outstanding_work_` | 未完成工作计数 | `== 0` 且 `op_queue_` 空 → `run()` 退出 |
| `stopped_` | 停止标记 | `stop()` 设置后，`run()` 立即返回 |
| `wakeup_event_` | 唤醒阻塞的 run 线程 | `post_immediate_completion()` 入队后调用 `wakeup_event_.unlock_and_signal_one()` |

### 5.4 completion_handler — 用户任务包装

```cpp
template <typename Handler, typename IoExecutor>
class completion_handler : public operation
{
public:
    completion_handler(Handler& h, const IoExecutor& io_ex)
        : operation(&completion_handler::do_complete),
          handler_(BOOST_ASIO_MOVE_CAST(Handler)(h)),
          work_(handler_, io_ex)
    {}

    static void do_complete(void* owner, operation* base,
        const boost::system::error_code& /*ec*/,
        std::size_t /*bytes_transferred*/)
    {
        completion_handler* h(static_cast<completion_handler*>(base));

        ptr p = { boost::asio::detail::addressof(h->handler_), h, h };

        Handler handler(BOOST_ASIO_MOVE_CAST(Handler)(h->handler_));
        p.h = boost::asio::detail::addressof(handler);
        p.reset();

        if (owner)
        {
            fenced_block b(fenced_block::half);
            boost_asio_handler_invoke_helpers::invoke(handler, handler);
        }
    }

private:
    Handler handler_;
    handler_work<Handler, IoExecutor> work_;
};
```

**包装关系与调用链路：**

```
用户代码:
  post(ioc, [](){ std::cout << "hello"; });
      │
      ▼ 1. handler 被包装
┌────────────────────────────┐
│ completion_handler          │
│  ┌──────────────────────┐   │
│  │ operation 基类        │   │
│  │  next_ = nullptr      │   │ ← 侵入式链表节点
│  │  func_ = do_complete  │───┼──┐
│  │  task_result_ = 0     │   │  │
│  └──────────────────────┘   │  │
│  handler_ = lambda          │  │ ← 用户回调存储
│  work_                      │  │ ← outstanding_work 管理
└────────────────────────────┘  │
                                 ▼
                          do_complete()
                            │
                            ├─ static_cast 还原派生类指针
                            ├─ ptr p (RAII 内存管理)
                            ├─ 取出 handler
                            ├─ p.reset() 释放 operation 内存
                            │
                            ▼
                          invoke(handler)
                            │
                            ▼
                          输出 "hello"
```

**关键：`do_complete` 中 `owner` 参数的双重语义**

| owner 值 | 语义 | 行为 |
|----------|------|------|
| `owner != 0` (scheduler 指针) | 正常执行 | 调用 `invoke(handler)` 执行用户回调 |
| `owner == 0` | 销毁 | 仅释放内存，不执行 handler |

### 5.5 call_stack — 线程局部调用栈

这是 `can_dispatch()` 判断的核心机制。

```cpp
template <typename Key, typename Value = unsigned char>
class call_stack
{
public:
    class context
    {
    public:
        explicit context(Key* k)
            : key_(k), next_(call_stack<Key, Value>::top_)
        {
            call_stack<Key, Value>::top_ = this;
        }

        ~context()
        {
            call_stack<Key, Value>::top_ = next_;
        }

    private:
        Key* key_;
        context* next_;
    };

    static Value* contains(Key* k)
    {
        context* elem = top_;
        while (elem)
        {
            if (elem->key_ == k)
                return elem->value_;
            elem = elem->next_;
        }
        return 0;
    }

private:
    static BOOST_ASIO_THREAD_LOCAL context* top_;
};
```

**调用栈追踪机制详解：**

```
线程 T 的 thread_local call_stack:
  
  top_ ──┐
         ▼
  ┌──────────────────────────┐
  │ context { key_=scheduler_A }│ ← run() 入口压入
  │ next_ ───────────────────┼─┐
  └──────────────────────────┘ │
                                ▼
  ┌──────────────────────────┐
  │ context { key_=scheduler_B }│ ← 另一个 ioc.run() 嵌套
  │ next_ = nullptr            │
  └──────────────────────────┘
```

**can_dispatch() 调用链路：**

```
dispatch(ioc, handler)
    │
    ▼
io_context::executor_type::dispatch(f, a)
    │
    ▼
scheduler::can_dispatch()
    │
    ▼
call_stack<scheduler, thread_info>::contains(this_scheduler)
    │
    ├─ 遍历当前线程的 thread_local call_stack
    │
    ├─ 找到 key_ == this_scheduler?
    │     │
    │     ├─ 是 → return value_ (非 0) → can_dispatch() == true
    │     │         → 立即同步执行 handler
    │     │
    │     └─ 否 → return 0 → can_dispatch() == false
    │             → 包装为 completion_handler 入队
```

**压栈时机：**

```
scheduler::run()
    │
    ├─ thread_info this_thread;
    │
    ├─ thread_call_stack::context ctx(this, this_thread);
    │   ← 构造 ctx 时，将 (this_scheduler, this_thread) 压入
    │   ← 当前线程的 thread_local call_stack
    │
    ├─ ... 执行事件循环 ...
    │
    └─ ctx 析构 → 从 call_stack 弹出
    │   ← run() 返回后，call_stack 不再包含此 scheduler
```

---

## 六、run() 内部逻辑与完整执行流程

### 6.1 run() 源码级流程分析

```cpp
std::size_t scheduler::run(boost::system::error_code& ec)
{
    // 步骤 1: 压入线程调用栈（使 can_dispatch() 能检测到此线程）
    thread_info this_thread;
    thread_call_stack::context ctx(this, this_thread);

    // 步骤 2: 加锁
    mutex::scoped_lock lock(mutex_);
    std::size_t n = 0;

    // 步骤 3: 主循环
    for (; do_run_one(lock, this_thread, ec); lock.lock())
        if (n != (std::numeric_limits<std::size_t>::max)())
            ++n;

    return n;
}
```

**调用链路：**

```
io_context::run()
    │
    ▼
scheduler::run(ec)
    │
    ├─ 1. thread_call_stack::context ctx(this, &this_thread)
    │      ← 压栈，使当前线程可被 can_dispatch() 检测
    │
    ├─ 2. mutex::scoped_lock lock(mutex_)
    │      ← 加锁，准备访问 op_queue_
    │
    ├─ 3. 循环调用 do_run_one()
    │      │
    │      ▼
    │   do_run_one(lock, this_thread, ec)
    │      │
    │      ├─ 检查 op_queue_ 是否有任务
    │      │     │
    │      │     ├─ 有任务 → 取出并执行 → return 1
    │      │     │
    │      │     ├─ 无任务且有 outstanding_work_ → 调用 reactor 等待
    │      │     │
    │      │     └─ 无任务且无 outstanding_work_ → return 0 (退出)
    │      │
    │      └─ lock.lock()  ← 每次循环重新加锁
    │
    └─ 4. ctx 析构 → call_stack 弹栈
```

### 6.2 do_run_one() 单步执行详解

```cpp
std::size_t scheduler::do_run_one(
    mutex::scoped_lock& lock,
    thread_info& this_thread,
    boost::system::error_code& ec)
{
    while (!stopped_)
    {
        if (op_queue_.empty())
        {
            // 分支 A: 队列空
            if (outstanding_work_ == 0)
            {
                // A1: 无未完成工作 → run() 应退出
                ec = boost::system::error_code();
                return 0;
            }

            // A2: 有未完成工作 → 需要等待 I/O 事件
            //     将 task_operation_ 标记入队（如未入队）
            if (!task_interrupted_)
            {
                op_queue_.push(&task_operation_);
                task_interrupted_ = true;
            }

            // 等待事件到来（释放锁，允许其他线程入队）
            wakeup_event_.clear(lock);
            lock.unlock();

            // A3: 调用 reactor 处理就绪事件
            if (task_->run(this_thread, lock, ec))
            {
                // reactor 返回了就绪事件
                // 将完成的 reactor_op push 到 op_queue_
                // 重新加锁后继续循环
                return 0;
            }
        }
        else
        {
            // 分支 B: 队列不空
            operation* op = op_queue_.front();
            op_queue_.pop();

            // B1: 如果取出的就是 task_operation_（reactor 占位符）
            if (op == &task_operation_)
            {
                task_interrupted_ = false;
                // 先将 reactor 移到队列末尾
                // 然后继续循环处理真正的任务
                op_queue_.push(&task_operation_);
                task_interrupted_ = true;
                continue;
            }

            // B2: 取出真正的用户任务
            // 释放锁，执行任务期间允许其他线程入队
            lock.unlock();

            // 执行 operation
            op->complete(this, ec, 0);
            ++n;

            // 重新加锁
            lock.lock();
            return 1;
        }
    }

    // stopped_ == true
    ec = boost::system::error_code();
    return 0;
}
```

**do_run_one() 完整决策流程图：**

```
do_run_one()
    │
    ├─ stopped_?
    │   ├─ 是 → return 0 (退出)
    │   └─ 否 → 继续
    │
    ├─ op_queue_.empty()?
    │   │
    │   ├─ 是 (队列空)
    │   │   │
    │   │   ├─ outstanding_work_ == 0?
    │   │   │   ├─ 是 → return 0 (无工作可做，退出)
    │   │   │   └─ 否 → 等待 I/O 事件
    │   │   │       │
    │   │   │       ├─ push task_operation_ (reactor 占位符)
    │   │   │       ├─ wakeup_event_.clear(lock) → 释放锁
    │   │   │       ├─ lock.unlock()
    │   │   │       ├─ task_->run() → reactor.wait()
    │   │   │       │   └─ epoll_wait() / IOCP GetQueuedCompletionStatus
    │   │   │       │   └─ 收集就绪事件
    │   │   │       │   └─ 将就绪的 reactor_op push 到 op_queue_
    │   │   │       │   └─ return
    │   │   │       │
    │   │   │       └─ 继续循环
    │   │   │
    │   └─ 否 (队列不空)
    │       │
    │       ├─ op = op_queue_.front(); pop();
    │       │
    │       ├─ op == task_operation_?
    │       │   ├─ 是 → push 回末尾，continue (跳过占位符)
    │       │   └─ 否 → 真正的用户任务
    │       │       │
    │       │       ├─ lock.unlock() → 释放锁
    │       │       │
    │       │       ├─ op->complete(this, ec, 0)
    │       │       │   └─ 调用 func_ 函数指针
    │       │       │   └─ completion_handler::do_complete
    │       │       │   └─ invoke(handler) → 执行用户回调
    │       │       │
    │       │       ├─ lock.lock() → 重新加锁
    │       │       │
    │       │       └─ return 1 (执行了一个任务)
```

### 6.3 run() 退出条件完整分析

| 条件 | outstanding_work_ | op_queue_ | 结果 |
|------|-------------------|-----------|------|
| 正常完成 | 0 | 空 | `run()` 返回 |
| 有 I/O 等待 | > 0 | 空 | 阻塞等待 reactor 事件 |
| 有任务待执行 | 任意 | 不空 | 取出执行 |
| 手动停止 | 任意 | 任意 | `stopped_ == true` → 立即返回 |

**outstanding_work_ 的变化时机：**

```
outstanding_work_++  (增加)
    │
    ├─ async_read/async_write 等异步操作发起时
    │   ← handler_work 构造时增加
    │
    ├─ make_work_guard() 时
    │   ← work_guard 构造时增加

outstanding_work_--  (减少)
    │
    ├─ handler 执行完毕后
    │   ← handler_work 析构时减少
    │
    ├─ work_guard.reset() 时
    │   ← work_guard 析构时减少
```

---

## 七、异步 I/O 完整调用链路

### 7.1 从 async_read 到 handler 执行的完整路径

```
用户层: socket.async_read_some(buffer, handler)
    │
    ▼ 1. 创建 async_read_operation
stream_handler_adapter(handler)
    │
    ▼ 2. 注册到 reactor
epoll_reactor::register_descriptor(socket.native_handle(), descriptor_state)
    │
    ▼ 3. 设置监控事件
epoll_reactor::enable_descriptor_operation(descriptor, read_op)
    │   └─ epoll_ctl(fd, EPOLL_CTL_ADD, socket_fd, EPOLLIN)
    │
    ▼ 4. 立即返回（不阻塞）
    │
    ▼ 5. run() 循环中等待
epoll_reactor::wait()
    │   └─ epoll_wait() → 返回就绪 fd 列表
    │
    ▼ 6. 处理就绪事件
for each ready fd:
    descriptor_state::perform_io(ec, bytes_transferred)
        │
        ├─ 调用 ::read() 或 ::recv() 读取数据
        │
        ├─ 将 reactor_op (async_read_operation) push 到
        │   scheduler::op_queue_
        │
        ▼ 7. 唤醒 run() 线程
wakeup_event_.unlock_and_signal_one()
    │
    ▼ 8. scheduler 从 op_queue_ 取出 reactor_op
    │
    ▼ 9. op->complete()
reactor_op::do_complete()
    │
    ├─ static_cast 还原为 async_read_operation
    │
    ▼ 10. invoke(handler)
handler(ec, bytes_transferred)  ← 用户回调执行
```

### 7.2 Reactor 事件通知机制

```
┌───────────────────────────────────────────────────┐
│           epoll_reactor::wait() 流程                │
├───────────────────────────────────────────────────┤
│                                                   │
│  1. epoll_wait(epoll_fd, events, max_events, timeout) │
│     │ ← 系统调用阻塞等待                          │
│     │ ← timeout 由 scheduler 计算                  │
│     │                                              │
│     ▼                                              │
│  2. 收集就绪事件                                    │
│     for (int i = 0; i < num_events; ++i):          │
│       │                                            │
│       ├─ 取出 descriptor_state (与 fd 关联)        │
│       │                                            │
│       ├─ descriptor_state::perform_io()            │
│       │   │                                        │
│       │   ├─ try_lock(descriptor_state.mutex)      │
│       │   │   ← 防止同一 fd 的多个事件并发处理      │
│       │   │                                        │
│       │   ├─ 执行实际 I/O 操作                     │
│       │   │   ::read() / ::recv() / ::write()      │
│       │   │                                        │
│       │   ├─ 将就绪的 reactor_op 入队              │
│       │   │   op_queue_.push(completion_ops)       │
│       │   │                                        │
│       │   └─ unlock(descriptor_state.mutex)        │
│       │                                            │
│       └─ 如果 descriptor_state 有更多等待的操作     │
│         再次 epoll_ctl 注册监控                     │
│                                                   │
│  3. 返回 scheduler，继续事件循环                    │
└───────────────────────────────────────────────────┘
```

**descriptor_state 的内部结构：**

```
descriptor_state (与一个 fd 关联)
    │
    ├── mutex_                    ← 内部锁，防止同一 fd 的并发处理
    │
    ├── read_op_queue_            ← 该 fd 的读操作队列
    │   └─ reactor_op (async_read)
    │
    ├── write_op_queue_           ← 该 fd 的写操作队列
    │   └─ reactor_op (async_write)
    │
    ├── except_op_queue_          ← 该 fd 的异常操作队列
    │
    └─ registered_events_         ← 当前注册到 epoll 的事件掩码
```

### 7.3 定时器异步等待链路

```
用户层: timer.async_wait(handler)
    │
    ▼ 1. 创建 timer_handler
deadline_timer_service::async_wait(timer, handler)
    │
    ▼ 2. 计算到期时间
    │   └─ 转换为 epoll/kqueue 的时间戳格式
    │
    ▼ 3. 注册到 reactor
epoll_reactor::add_timer_queue(deadline_timer_service)
    │
    ▼ 4. 插入定时器队列
timer_queue::enqueue(timer_op, expiry_time)
    │   └─ 按到期时间排序的优先队列
    │
    ▼ 5. run() 循环中
scheduler::do_run_one()
    │   └─ 队列空时调用 task_->run()
    │
    ▼ 6. reactor 处理定时器
epoll_reactor::run()
    │   ├─ 计算最近的到期时间作为 epoll_wait 的 timeout
    │   ├─ epoll_wait(epoll_fd, events, max, min_timeout)
    │   ├─ 检查 timer_queue 是否有到期定时器
    │   │   timer_queue::get_ready_timers()
    │   │   │
    │   │   ├─ 取出所有到期时间 <= now 的 timer_op
    │   │   │
    │   │   ▼ 7. push 到 scheduler.op_queue_
    │   │   op_queue_.push(timer_op)
    │   │
    │   └─ 返回到 scheduler
    │
    ▼ 8. scheduler 执行 timer_op
timer_op->complete()
    │
    ▼ 9. invoke(handler)
handler(ec)  ← 用户回调执行（ec 通常为 success）
```

**定时器到期后的完整时序图：**

```
时间轴:
  T0: async_wait() 发起 → 注册到 reactor
  T0-T+2: run() 空闲等待（epoll_wait timeout = 2s）
  T+2: 定时器到期

  T+2时刻的内部动作:
    epoll_wait 返回 (timeout 触发)
    │
    ▼
  timer_queue::get_ready_timers()
    │   └─ 发现到期时间 <= now
    │
    ▼
  取出 timer_op，push 到 op_queue_
    │
    ▼
  wakeup run() 线程
    │
    ▼
  op->complete() → do_complete()
    │
    ▼
  invoke(handler)
    │
    ▼
  用户回调: handler(error_code::success)
```

---

## 八、dispatch 与 post 深度分析

### 8.1 核心区别对比

| 特性 | dispatch | post |
|------|----------|------|
| 是否可能立即执行 | 可能立即执行 | 绝不立即执行 |
| 执行时机 | `can_dispatch()==true` 时同步调用 | 总是排队，异步调用 |
| 是否阻塞当前流程 | 可能阻塞 | 不阻塞 |
| `is_continuation` 标志 | false | true |
| 递归安全性 | 有递归风险 | 安全 |
| 适用场景 | 优化性能，减少排队 | 保证不重入，延迟执行 |

**核心一句话：**
- **dispatch**：如果当前线程已经在 executor 上下文中运行，则立即执行；否则排队。
- **post**：永远不会立即执行，总是放入队列，等待后续处理。

### 8.2 dispatch 源码逻辑与调用链路

```cpp
template <typename Function, typename Allocator>
void io_context::executor_type::dispatch(Function&& f, const Allocator& a) const
{
    if (impl_.can_dispatch())
    {
        // 立即执行分支
        fenced_block b(fenced_block::full);
        boost_asio_handler_invoke_helpers::invoke(f, f);
        return;
    }

    // 排队执行分支
    typedef detail::completion_handler<...> op;
    op* p = ...;
    impl_.post_immediate_completion(p, false);
}
```

**dispatch 完整调用链路图：**

```
dispatch(ioc, handler)
    │
    ▼
io_context::executor_type::dispatch(f, a)
    │
    ▼
scheduler::can_dispatch()
    │
    ▼
call_stack<scheduler>::contains(this_scheduler)
    │
    ├─ 找到 (当前线程在 run() 中)
    │   │
    │   ▼ 立即执行分支
    │   fenced_block b(fenced_block::full)
    │       ← 阻止某些中断/信号干扰
    │   │
    │   ▼
    │   invoke(handler)
    │       ← 同步调用，栈上直接执行
    │   │
    │   ▼
    │   handler 执行完毕
    │   ← fenced_block 析构，恢复状态
    │
    └─ 未找到 (当前线程不在 run() 中)
        │
        ▼ 排队执行分支
        1. 构造 completion_handler 对象
           │  func_ = do_complete
           │  handler_ = f
           │
        2. scheduler::post_immediate_completion(op, false)
           │  is_continuation = false
           │
           ├─ mutex::scoped_lock lock(mutex_)
           │
           ├─ op_queue_.push(op)
           │
           ├─ wakeup_event_.unlock_and_signal_one(lock)
           │   ← 唤醒正在 run() 中等待的线程
           │
           ▼
           后续由 run() 线程取出并执行
```

### 8.3 post 源码逻辑与调用链路

```cpp
template <typename Function, typename Allocator>
void io_context::executor_type::post(Function&& f, const Allocator& a) const
{
    typedef detail::completion_handler<...> op;
    op* p = ...;

    impl_.post_immediate_completion(p, true);
}
```

**post 完整调用链路图：**

```
post(ioc, handler)
    │
    ▼
io_context::executor_type::post(f, a)
    │
    ▼
1. 构造 completion_handler 对象
   │  func_ = do_complete
   │  handler_ = f
   │  work_ 构造 → outstanding_work_++ (增加工作计数)
   │
   ▼
2. scheduler::post_immediate_completion(op, true)
   │  is_continuation = true
   │
   ├─ mutex::scoped_lock lock(mutex_)
   │
   ├─ op_queue_.push(op)
   │
   ├─ wakeup_event_.unlock_and_signal_one(lock)
   │   ← 唤醒等待的 run() 线程
   │
   ▼
   后续由 run() 线程执行:
     do_run_one() → op->complete()
       │
       ▼
     completion_handler::do_complete()
       │
       ├─ 取出 handler
       ├─ p.reset() → 释放 completion_handler 内存
       │                 → outstanding_work_-- (减少工作计数)
       │
       ▼
     invoke(handler)
       │
       ▼
     handler 执行完毕
```

### 8.4 can_dispatch 关键澄清

**常见误区：** "调用 dispatch 的线程，不应该一定是在运行这个 io_context 吗？"

**答案：不。** dispatch 可以被任何线程调用，不限于运行 io_context 的线程。

**两种完全不同的线程角色：**

```
┌─────────────────────────────────────────────────────┐
│  角色1：投递任务的线程（调用 dispatch/post 的线程）      │
│         - 可以是任何线程                                │
│         - 包括没有运行 io_context 的线程                │
├─────────────────────────────────────────────────────┤
│  角色2：运行 io_context 的线程（调用 run() 的线程）      │
│         - 正在执行事件循环                              │
│         - 正在从队列取任务并执行                         │
└─────────────────────────────────────────────────────┘
```

**can_dispatch() 检查的是：** "调用 dispatch 的这个线程，是否同时也是角色2（正在 run）？"

**为什么这样设计？**

dispatch 的语义保证是："尽可能快地执行，如果安全的话就立即执行"。

"安全"的含义：只有当前线程已经在 io_context 的执行上下文中时，立即调用才不会破坏线程模型（不会引发数据竞争）。

如果不检查：
```cpp
// 假设 dispatch 不检查，直接执行
boost::asio::io_context ioc;
std::thread worker([&]{ ioc.run(); });

// 主线程直接执行任务
boost::asio::dispatch(ioc, []{
    // 这个任务在主线程执行了！
    // → 破坏了"所有 ioc 任务都在 run 线程执行"的假设
    // → 可能引发数据竞争
});
```

### 8.5 四种调用场景与调用栈追踪

**场景1：在 run() 线程内部调用 dispatch → 立即执行**

```cpp
ioc.post([&]{
    boost::asio::dispatch(ioc, []{
        std::cout << "立即执行\n";
    });
});
ioc.run();
```

```
线程T 的调用栈：
  main()
    └─ ioc.run()              ← 角色2：正在运行 ioc
         │
         ├─ thread_call_stack::context ctx(this_scheduler)
         │   ← 压栈：scheduler 指针进入 thread_local
         │
         └─ 执行外层 lambda
              │
              └─ dispatch()
                   │
                   └─ can_dispatch()?
                      │
                      └─ call_stack::contains(this_scheduler)?
                         │
                         └─ 当前栈里有 ioc.run() 吗? → 有！
                            │
                            └─ ctx 存在 → 找到 → true
                            │
                            ▼ 立即执行内层 lambda
```

**场景2：在 run() 线程外部调用 dispatch → 排队**

```cpp
std::thread worker([&]{ ioc.run(); });
boost::asio::dispatch(ioc, []{
    std::cout << "排队执行\n";
});
```

```
主线程的调用栈：              线程A 的调用栈：
  main()                       ioc.run()
    └─ dispatch()                │
         │                       ├─ ctx 压栈
         └─ can_dispatch()?      └─ 等待/执行任务
            │
            └─ call_stack::contains(ioc_scheduler)?
               │
               └─ 主线程的 thread_local 栈中没有 ioc.run()
                  │
                  └─ → false → 排队，由线程A执行
```

**场景3：完全没有线程在运行 io_context → 排队**

```cpp
boost::asio::io_context ioc;
boost::asio::dispatch(ioc, []{ /* ... */ });
// can_dispatch() == false，因为没人在 run
ioc.run();
```

**场景4：在另一个 io_context 的 run 线程中调用 → 排队**

```cpp
boost::asio::io_context ioc1, ioc2;

ioc1.post([&]{
    boost::asio::dispatch(ioc2, []{ /* ... */ });
});
ioc1.run();
```

```
线程T 的调用栈：
  ioc1.run()
    ├─ ctx1 压栈 (key = scheduler_ioc1)
    │
    └─ 执行 lambda
         └─ dispatch(ioc2, handler)
              │
              └─ ioc2.can_dispatch()?
                 │
                 └─ call_stack::contains(scheduler_ioc2)?
                    │
                    └─ 线程栈中只有 scheduler_ioc1，没有 scheduler_ioc2
                       │
                       └─ → false → 排队到 ioc2
```

**dispatch 执行结果对比（场景1中的关键示例）：**

```cpp
io_context ioc;

ioc.post([&]{
    std::cout << "1 start\n";
    boost::asio::dispatch(ioc, []{ std::cout << "dispatch\n"; });
    boost::asio::post(ioc, []{ std::cout << "post\n"; });
    std::cout << "1 end\n";
});

ioc.run();
```

输出：

```
1 start
dispatch     ← dispatch 立即执行（已在 run 线程上下文中）
1 end
post         ← post 排队后执行
```

```
执行栈演示：
io_context::run()
  └─ 执行外层 handler "1 start"
       ├─ dispatch()
       │    └─ can_dispatch() == true
       │         └─ 直接调用 → 输出 "dispatch"   [同步嵌套执行]
       ├─ post()
       │    └─ 直接入队（不执行）
       └─ 输出 "1 end"
  └─ 继续处理队列
       └─ 执行 post 的 handler → 输出 "post"
```

### 8.6 is_continuation 标志与调度优化

```cpp
void scheduler::post_immediate_completion(
    operation* op, bool is_continuation)
{
    if (one_thread_ || is_continuation)
    {
        // 单线程模式 或 continuation 任务
        // 放入线程本地缓存（更快的访问）
        ...
    }

    mutex::scoped_lock lock(mutex_);
    op_queue_.push(op);
    wake_one_thread_and_unlock(lock);
}
```

| 标志值 | 语义 | 来源 | 调度策略 |
|--------|------|------|----------|
| `false` | 非延续任务 | dispatch 排队时 | 可能走全局队列 |
| `true` | 延续/独立任务 | post 调用时 | 可能走线程本地优化队列 |

**is_continuation 的本质含义：**

- `true` (post)：这是一个全新的、独立的任务，从某个外部源发起
- `false` (dispatch 排队)：这是当前执行链的一部分，有逻辑上的延续关系

在单线程模式下（`one_thread_ == true`），continuation 任务可以放入线程本地缓存，避免全局锁竞争。

---

## 九、多线程使用模型

### 9.1 线程池模式

```cpp
#include <boost/asio.hpp>
#include <thread>
#include <vector>

int main()
{
    boost::asio::io_context ioc;

    for (int i = 0; i < 10; ++i) {
        boost::asio::post(ioc, [i]{
            std::cout << "Task " << i
                      << " on thread "
                      << std::this_thread::get_id() << "\n";
        });
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&ioc]{
            ioc.run();
        });
    }

    for (auto& t : threads) t.join();
    return 0;
}
```

```
        io_context (共享队列)
       /      |      |      \
   线程1    线程2   线程3    线程4
   run()    run()   run()   run()
       \      |      |      /
        共同从队列取任务执行
        （自动负载均衡）
```

### 9.2 线程安全机制分析

```
多线程 run() 的锁交互时序:

  线程A (run):                线程B (run):                线程C (post):
    │                           │                           │
    ├─ lock(mutex_)             │                           │
    ├─ op_queue_.front()        │                           │
    ├─ op_queue_.pop()          │                           │
    ├─ lock.unlock()            │                           │
    │                           ├─ lock(mutex_) ← 等待锁    ├─ lock(mutex_) ← 等待锁
    ├─ op->complete()           │                           │
    │   └─ 执行 handler         │                           │
    ├─ lock.lock()              │                           │
    │                           ├─ lock 获取 ←              ├─ lock 获取 ←
    │                           ├─ op_queue_.front()         ├─ op_queue_.push(op)
    │                           ├─ op_queue_.pop()           ├─ wake_one_thread_and_unlock
    │                           ├─ lock.unlock()             │
    │                           ├─ op->complete()            │
```

**关键点：**
- `run()` 在取任务前加锁，执行任务时释放锁
- `post()` 入队时加锁，入队后唤醒等待线程
- handler 执行期间锁被释放，允许其他线程并发操作队列

### 9.3 strand 串行化保证

当多个 handler 需要串行执行（避免并发访问共享数据），使用 strand：

```cpp
boost::asio::io_context ioc;
boost::asio::strand<boost::asio::io_context::executor_type> strand(ioc.get_executor());

// 即使多线程 run(), strand 保证这些 handler 串行执行
for (int i = 0; i < 5; ++i) {
    boost::asio::post(strand, [i]{
        // 安全访问共享数据，无需加锁
        std::cout << "Strand task " << i << "\n";
    });
}

std::vector<std::thread> threads;
for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&ioc]{ ioc.run(); });
}
for (auto& t : threads) t.join();
```

**strand 的内部机制：**

```
strand 内部有自己的等待队列 (strand_queue_)
    │
    ├─ 当 strand 未在执行时:
    │   post/dispatch → 包装为 strand_handler
    │   → 通过 strand 的内部锁串行化
    │   → 只有第一个 handler 被 post 到 io_context
    │
    ├─ 当 strand 正在执行时:
    │   新 handler 只进入 strand 的等待队列
    │   不会直接 post 到 io_context
    │
    └─ 当前 handler 执行完毕后:
        strand 从等待队列取出下一个
        → dispatch 到 io_context 执行
        （因为 strand 在 run 线程上下文中，
         dispatch 会立即执行 → 无排队开销）
```

---

## 十、work_guard 防止提前退出

```cpp
#include <boost/asio.hpp>

int main()
{
    boost::asio::io_context ioc;

    auto work = boost::asio::make_work_guard(ioc);

    std::thread t([&ioc]{
        ioc.run();
    });

    boost::asio::post(ioc, []{ /* ... */ });

    work.reset();
    t.join();
    return 0;
}
```

**work_guard 的内部机制：**

```
make_work_guard(ioc)
    │
    ▼
构造 executor_work_guard<io_context::executor_type>
    │
    ├─ 获取 ioc.get_executor()
    │
    ├─ 调用 executor 的 on_work_started()
    │   └─ scheduler::outstanding_work_++
    │   ← 增加工作计数，阻止 run() 因 "outstanding_work_ == 0" 退出
    │
    └─ 保存 executor 引用

work.reset()
    │
    ▼
executor_work_guard 析构
    │
    ├─ 调用 executor 的 on_work_finished()
    │   └─ scheduler::outstanding_work_--
    │
    └─ run() 处理完剩余任务后检测到 outstanding_work_ == 0 → 退出
```

---

## 十一、设计模式总结

| 设计点 | 采用方式 | 优势 |
|--------|----------|------|
| 任务抽象 | operation 基类 + 函数指针 | 避免虚表开销，统一执行/销毁 |
| 任务队列 | 侵入式链表 op_queue | O(1) 操作，无额外内存分配 |
| 平台适配 | reactor 抽象层（Reactor 模式） | 跨平台 epoll/IOCP/kqueue |
| 线程上下文检测 | thread_local 调用栈 | dispatch 优化判断 |
| 内存管理 | RAII + handler_alloc | 自定义分配器优化 |
| 并发模型 | 多线程共享队列 + 互斥锁 | 自动负载均衡 |
| 串行化 | strand 机制 | 无锁串行化，避免数据竞争 |
| 退出控制 | outstanding_work_ 计数 | work_guard 防止提前退出 |

---

## 十二、使用建议与陷阱

### 12.1 dispatch 的递归风险

```cpp
// 危险：可能导致深度递归甚至栈溢出
void handler() {
    boost::asio::dispatch(ioc, &handler); // 在 run 线程内，立即递归！
}
```

应改用 post：

```cpp
void handler() {
    boost::asio::post(ioc, &handler); // 总是入队，栈不会增长
}
```

### 12.2 post 保证"不重入"

如果需要保证 handler 一定在干净的栈上独立执行（例如保护临界区不被重入），用 post。

### 12.3 dispatch 的性能优势

dispatch 减少了一次入队/出队和潜在的线程唤醒开销，在确定不会递归的场景下性能更优。

### 12.4 选择原则

```
┌─────────────────────────────────────────────────┐
│  • 需要避免重入/递归        → 用 post              │
│  • 追求性能且确定安全       → 用 dispatch          │
│  • 需要保证异步语义         → 用 post              │
│  • 在 run 线程内想快速执行   → 用 dispatch          │
└─────────────────────────────────────────────────┘
```

### 12.5 run() 在无任务时的行为

- `outstanding_work_ == 0` 且 `op_queue_` 空 → `run()` 立即返回
- `outstanding_work_ > 0` 且 `op_queue_` 空 → `run()` 阻塞等待 I/O 事件
- 使用 `work_guard` 可确保 `run()` 不提前退出

### 12.6 io_context::stop() 的行为

`stop()` 设置 `stopped_ = true`，导致所有 `run()` 线程退出。但已入队的 handler 不会被执行。需要 `restart()` 才能再次 `run()`。

---

## 附录：核心概念速查

### io_context 总结

```
┌─────────────────────────────────────────────────┐
│  io_context 是 Boost.Asio 的"心脏"                 │
│                                                   │
│  • 它是一个任务调度器 + I/O 事件循环                │
│                                                   │
│  • post/dispatch  → 把任务放入它的队列              │
│  • run()          → 启动循环，执行队列中的任务       │
│                     并等待 I/O 事件                 │
│                                                   │
│  • 底层封装了 epoll/IOCP/kqueue                    │
│  • 支持单线程和多线程模型                           │
│                                                   │
│  本质：让你用统一的方式编写跨平台、高性能的           │
│        异步程序                                    │
└─────────────────────────────────────────────────┘
```

### dispatch vs post 终极对照

| 维度 | dispatch | post |
|------|----------|------|
| 立即执行可能性 | 在 run 线程内调用时立即执行 | 永不立即执行 |
| can_dispatch 检查 | 有 | 无 |
| 入队标志 is_continuation | false | true |
| 递归安全性 | 有递归风险 | 安全 |
| 性能 | 可能更优（省去排队） | 多一次排队开销 |
| 典型用途 | 性能优化、链式调用 | 防重入、解耦、保证异步 |

### 关键调用链路速查

| 路径 | 起点 | 终点 | 关键步骤 |
|------|------|------|----------|
| post 执行 | `post(ioc, h)` | `h()` 执行 | 包装→入队→run取出→complete→invoke |
| dispatch 立即 | `dispatch(ioc, h)` (run线程内) | `h()` 执行 | can_dispatch检查→true→同步invoke |
| dispatch 排队 | `dispatch(ioc, h)` (非run线程) | `h()` 执行 | can_dispatch检查→false→包装→入队→run取出→invoke |
| async_read | `socket.async_read_some(buf, h)` | `h(ec, bytes)` | 注册到reactor→epoll_wait→perform_io→入队→complete→invoke |
| timer | `timer.async_wait(h)` | `h(ec)` | 插入timer_queue→epoll_wait timeout→到期→入队→complete→invoke |

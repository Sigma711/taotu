# taotu

_[English](README.md) | [简体中文](README_zh-Hans.md)_

!["taotu" logo](./img/taotu.jpg)

一个基于并发 Reactor 模型的轻量级 C++ 网络库，支持 io_uring，并附带可直接运行的示例。

## 构建

```bash
cmake -S . -B build
cmake --build build -j
```

可选的 Release 构建：

```bash
cmake -S . -B build_release -DCMAKE_BUILD_TYPE=Release
cmake --build build_release -j
```

说明：
- 需要 C++17 编译器、CMake 和 liburing。
- RPC 示例需要 protobuf。

## 配置项

### CMake 选项

可通过以下参数配置构建行为：

- `-DCMAKE_BUILD_TYPE=Release|Debug`（或其他 CMake 构建类型）
- `-DTAOTU_ENABLE_CLANG_TIDY=ON|OFF`（默认 `OFF`）
- `-DTAOTU_ENABLE_CLANG_FORMAT=ON|OFF`（默认 `OFF`）

### 运行时环境变量（核心 io_uring 后端）

以下变量会在进程启动时由 `Poller` 读取：

- `TAOTU_ENABLE_BUF_RING`
  - 默认：开启（尽力启用）
  - 作用：启用 io_uring 的 buffer ring（`buf_ring`）作为 provided-buffer 的回收机制
    （值非空且不为 `'0'` 时启用）。当开启且系统内核/liburing 支持时，recv-multishot
    的 buffer 归还会变成纯用户态操作（不再需要额外提交 `IORING_OP_PROVIDE_BUFFERS` SQE）。
- `TAOTU_DISABLE_BUF_RING`
  - 默认：关闭
  - 作用：强制关闭 `buf_ring` 的 provided-buffer 路径，回退到旧的
    `IORING_OP_PROVIDE_BUFFERS` 机制（值非空且不为 `'0'` 时禁用），优先级高于 `TAOTU_ENABLE_BUF_RING`。
- `TAOTU_IORING_ENTRIES`
  - 默认值：`32768`
  - 限制范围：`[1024, 32768]`
  - 作用：设置 io_uring 队列深度（调小可降低内存占用）。
- `TAOTU_ENABLE_SQPOLL`
  - 默认：关闭
  - 作用：请求启用 io_uring 的 SQPOLL 模式（值非空且不为 `'0'` 时启用）。
- `TAOTU_DISABLE_SQPOLL`
  - 默认：关闭
  - 作用：强制关闭 SQPOLL（值非空且不为 `'0'` 时关闭），优先级高于 `TAOTU_ENABLE_SQPOLL`。
- `TAOTU_DISABLE_RECV_MULTISHOT`
  - 默认：关闭
  - 作用：关闭 recv-multishot + provided-buffer 注册路径。
- `TAOTU_IORING_SUBMIT_BATCH`
  - 默认值：`16`
  - 最大限制：`256`
  - 作用：当待提交 SQE 数达到该阈值时触发提交（事件循环中也可能被强制提交）。
- `TAOTU_IORING_OP_POOL_LIMIT`
  - 默认值：`65536`
  - 最大限制：`1048576`
  - 作用：每个 `Poller` 可缓存复用的 io_uring 操作对象上限。
- `TAOTU_IORING_BORROWED_BUFFER_LIMIT`
  - 默认值：`kBufCount/2`（当前为 `128`）
  - 限制范围：`[0, kBufCount]`
  - 作用：recv-multishot 的 provided-buffer 最多允许被“借用/持有”的数量上限
    （读取 CQE 回调结束后暂不归还，用于借用直发）；设为 `0` 可禁用借用
    （借用发送会自动回退为拷贝发送）。

示例：

```bash
TAOTU_IORING_ENTRIES=16384 \
TAOTU_IORING_SUBMIT_BATCH=16 \
./pingpong_server 4567 8
```

## 运行示例

可执行文件位于 `build/output/bin`（或 `build_release/output/bin`）。每个示例在 `example/` 目录下都有独立 README。

示例：

```bash
cd build/output/bin
./simple_echo 4567 4
```

### Demo 命令行参数

服务端示例：

- `simple_echo [port [io_threads]]`
- `simple_discard [port [io_threads]]`
- `simple_time [port [io_threads]]`
- `http_server [port [io_threads]]`
- `chat_server [port [io_threads]]`
- `pingpong_server [port [io_threads]]`

客户端示例：

- `chat_client [host_ip] <port>`（仅提供 `host_ip` 时，端口默认为 `4567`）
- `pingpong_client <host_ip> <port> <threads> <block_size> <sessions> <time_sec>`
- `time_service_sync_client`（无命令行参数）

RPC 服务端示例：

- `time_service_server`（无命令行参数）

## 基本使用

一般流程：
1. 创建 `EventManager`（或线程池）。
2. 创建 `Server`/`Client` 或示例封装。
3. 注册连接/消息/写完成/关闭回调。
4. 启动事件循环。

`example/` 目录提供了最小可运行的模式参考。

# taotu

_[English](README.md) | [简体中文](README_zh-Hans.md)_

!["taotu" logo](./img/taotu.jpg)

A lightweight C++ network library based on the concurrent Reactor model, with io_uring support and a set of runnable demos.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

Optional Release build:

```bash
cmake -S . -B build_release -DCMAKE_BUILD_TYPE=Release
cmake --build build_release -j
```

Notes:
- Requires a C++17 compiler, CMake, and liburing.
- RPC demo uses protobuf.

## Configuration

### CMake options

You can configure build behavior with:

- `-DCMAKE_BUILD_TYPE=Release|Debug` (or other CMake build types)
- `-DTAOTU_ENABLE_CLANG_TIDY=ON|OFF` (default `OFF`)
- `-DTAOTU_ENABLE_CLANG_FORMAT=ON|OFF` (default `OFF`)

### Runtime environment variables (core io_uring backend)

These are read at process startup by `Poller`:

- `TAOTU_IORING_ENTRIES`
  - Default: `32768`
  - Clamp range: `[1024, 32768]`
  - Effect: io_uring queue depth (can reduce memory footprint when lowered).
- `TAOTU_ENABLE_SQPOLL`
  - Default: disabled
  - Effect: request io_uring SQPOLL mode (`non-empty` and not `'0'` enables).
- `TAOTU_DISABLE_SQPOLL`
  - Default: disabled
  - Effect: force-disable SQPOLL (`non-empty` and not `'0'` disables), takes precedence over `TAOTU_ENABLE_SQPOLL`.
- `TAOTU_DISABLE_RECV_MULTISHOT`
  - Default: disabled
  - Effect: disable recv-multishot + provided-buffer registration path.
- `TAOTU_IORING_SUBMIT_BATCH`
  - Default: `1`
  - Max clamp: `256`
  - Effect: submit SQEs when pending count reaches this threshold (or when forced by the loop).
- `TAOTU_IORING_OP_POOL_LIMIT`
  - Default: `65536`
  - Max clamp: `1048576`
  - Effect: max cached io_uring operation objects per `Poller` for allocation reuse.
- `TAOTU_IORING_BORROWED_BUFFER_LIMIT`
  - Default: `kBufCount/2` (currently `128`)
  - Clamp range: `[0, kBufCount]`
  - Effect: max number of recv-multishot provided buffers that can be leased
    (held past the read CQE) for the borrowed-send fast path; `0` disables
    leasing (borrowed send falls back to copy).

Example:

```bash
TAOTU_IORING_ENTRIES=16384 \
TAOTU_IORING_SUBMIT_BATCH=16 \
./pingpong_server 4567 8
```

## Run demos

Binaries are placed under `build/output/bin` (or `build_release/output/bin`). Each demo has its own README under `example/`.

Example:

```bash
cd build/output/bin
./simple_echo 4567 4
```

### Demo command-line options

Server demos:

- `simple_echo [port [io_threads]]`
- `simple_discard [port [io_threads]]`
- `simple_time [port [io_threads]]`
- `http_server [port [io_threads]]`
- `chat_server [port [io_threads]]`
- `pingpong_server [port [io_threads]]`

Client demos:

- `chat_client [host_ip] <port>` (if only `host_ip` is provided, port defaults to `4567`)
- `pingpong_client <host_ip> <port> <threads> <block_size> <sessions> <time_sec>`
- `time_service_sync_client` (no CLI args)

RPC server demo:

- `time_service_server` (no CLI args)

## Basic usage

High-level flow:
1. Create an `EventManager` (or a pool of them).
2. Create `Server`/`Client` or demo-specific wrappers.
3. Register callbacks for connection/message/write/close events.
4. Start the event loop.

The `example/` directory contains minimal servers/clients showing common patterns.

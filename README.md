# Pooltergeist

[![Build](https://github.com/ousatov-ua/pooltergeist/actions/workflows/maven.yml/badge.svg)](https://github.com/ousatov-ua/pooltergeist/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.ousatov-ua/pooltergeist)](https://central.sonatype.com/artifact/io.github.ousatov-ua/pooltergeist)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Visitors](https://visitor-badge.laobi.icu/badge?page_id=ousatov-ua.pooltergeist)](https://github.com/ousatov-ua/pooltergeist)
[![GitHub commits](https://img.shields.io/github/commit-activity/t/ousatov-ua/pooltergeist)](https://github.com/ousatov-ua/pooltergeist/commits/main)
[![GitHub last commit](https://img.shields.io/github/last-commit/ousatov-ua/pooltergeist)](https://github.com/ousatov-ua/pooltergeist/commits/main)

![img](https://raw.githubusercontent.com/ousatov-ua/pooltergeist/refs/heads/main/img/pooltergeist_1920x960.png)

> A Java 25+ multithreading utility library that brings order to the chaos of thread pool management.

Pooltergeist separates CPU-bound, IO-bound, and virtual-thread work into purpose-built executor pools, adds bounded-concurrency backpressure via a bulkhead, and provides a high-throughput `TaskManager` for fan-out processing pipelines — all wired up and ready to use in three lines of code.


You can use the code in this repo as-is, or fork it and customize it to your needs.
Please submit any issues or pull requests.

---

## Maven

```xml
<dependency>
  <groupId>io.github.ousatov-ua</groupId>
  <artifactId>pooltergeist</artifactId>
  <version><<!-- see latest on Maven Central --></version>
</dependency>
```

---

## ExecutorHub — one hub, three pools

`ExecutorHub` creates and manages three named executor pools:

| Room | Executor | Best for |
|------|----------|----------|
| **Attic** (`virtual()`) | Virtual-thread-per-task | Lightweight async, lots of tasks |
| **Ballroom** (`cpu()`) | ForkJoinPool sized to cores | CPU-intensive computation |
| **Dungeon** (`io()`) | BulkheadedExecutor (virtual threads) | IO / blocking calls with concurrency cap |

```java
var config = ExecHubConfig.builder()
    .ioMaxInFlight(64)          // max simultaneous IO tasks
    .cpuPoolCoresDelimiter(2)   // parallelism = cores / 2
    .cpuPoolMinSize(2)          // at least 2 CPU threads
    .maxWaitTimeoutSeconds(30)  // graceful shutdown timeout
    .build();

try (var hub = new ExecutorHub(config)) {

    // CPU-bound: transform a list in parallel
    List<String> upper = hub.cpu().executeTasksBlocking(names, String::toUpperCase);

    // IO-bound: fire requests and collect results (backpressure included)
    List<Response> responses = hub.io().executeTasksBlocking(urls, this::fetchUrl);

    // Virtual threads: fire-and-forget
    hub.virtual().submitTasks(() -> log.info("Hello from virtual thread"));

} // hub.close() drains all pools concurrently
```

---

## BulkheadedExecutor — virtual threads with a concurrency cap

Use `BulkheadedExecutor` when you need virtual threads but must limit the number of
simultaneous in-flight operations (e.g., downstream connection pool size).

```java
// Allow at most 20 concurrent tasks
var executor = new BulkheadedExecutor(20);

Future<String> f = executor.submit(() -> callExternalService());
executor.execute(() -> fireAndForget());

// Runtime metrics
System.out.println("In flight : " + executor.getInFlight());
System.out.println("Waiting   : " + executor.getWaiting());
System.out.println("Completed : " + executor.getCompleted());

executor.shutdown();
executor.awaitTermination(10, TimeUnit.SECONDS);
```

You can also inject an existing `ExecutorService` and choose fair semaphore ordering:

```java
var executor = new BulkheadedExecutor(
    Executors.newVirtualThreadPerTaskExecutor(),
    /*maxInFlight=*/ 10,
    /*fair=*/        true
);
```

---

## TaskManager — high-throughput fan-out pipelines

`TaskManager<T, R>` reads work units from a bounded deque, fans them out to a
configurable thread pool, and collects results with optional error tracking.

### Step 1 — implement `WorkUnit`

```java
@Builder
record Order(long id, String payload) implements WorkUnit {

    // Sentinel value — identity equality is used by TaskManager
    public static final Order LAST = new Order(-1, "");

    @Override public WorkUnit getLastWorkUnit() { return LAST; }
    @Override public String getType()           { return "order"; }
}
```

### Step 2 — subclass `TaskManager` (optional, for error detection)

```java
class OrderProcessor extends TaskManager<Order, ProcessResult> {

    OrderProcessor(TaskManagerConfig cfg) {
        super(cfg, order -> processOrder(order));
    }

    @Override
    protected boolean isInError(ProcessResult result) {
        return !result.success();
    }
}
```

### Step 3 — submit work and wait

```java
var config = TaskManagerConfig.builder()
    .eventProcessingParallelism(8)   // parallel workers
    .workUnitsDequeSize(500)         // bounded input buffer
    .tasksDequeSize(500)             // bounded task buffer
    .waitTimeForAllTasksFinishedMinute(5)
    .build();

try (var processor = new OrderProcessor(config)) {

    for (Order order : fetchOrders()) {
        processor.submit(order);     // blocks when buffer is full
    }

    // Signal end-of-stream and wait for all tasks to complete
    processor.waitForCompletion(Order.LAST);
    processor.logStatistics();
}
```

---

## Configuration reference

### ExecHubConfig

| Field | Default | Description |
|-------|---------|-------------|
| `ioMaxInFlight` | `64` | Max simultaneous IO tasks (bulkhead permits) |
| `cpuPoolCoresDelimiter` | `1` | CPU parallelism = `availableProcessors / delimiter` |
| `cpuPoolMinSize` | `2` | Minimum CPU thread pool size |
| `maxWaitTimeoutSeconds` | `300` | Shutdown await timeout per pool |

### TaskManagerConfig

| Field | Default | Description |
|-------|---------|-------------|
| `eventProcessingParallelism` | `20` | Number of worker threads |
| `workUnitsDequeSize` | `200` | Bounded input queue capacity |
| `tasksDequeSize` | `200` | Bounded task queue capacity |
| `logForRecordCount` | `100` | Log progress every N records per type |
| `waitTimeForAllTasksFinishedMinute` | `30` | Max wait for all tasks to finish |
| `waitTimeForCheckingFinishedSeconds` | `10` | Polling interval during shutdown |

---

## Why "Pooltergeist"?

Because thread pools, like poltergeists, are invisible forces that cause chaos when
left unmanaged. Pooltergeist names its executor rooms after parts of a haunted house:

- **Attic** — virtual threads floating above the rest, lightweight and plentiful
- **Ballroom** — CPU work, the heart of the house where the real action happens
- **Dungeon** — IO-bound tasks, kept isolated underground with a strict concurrency cap

---

## License

[MIT](https://opensource.org/licenses/MIT) © Oleksii Usatov

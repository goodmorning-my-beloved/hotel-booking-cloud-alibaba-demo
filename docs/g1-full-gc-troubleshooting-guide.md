# G1 频繁 Full GC 排查快速手册

本手册配合本工程的 `jvm-gc` lab 使用。目标不是背 JVM 参数，而是建立一套可落地的排查路径：先用 Grafana 判断现象，再用 GC 日志确认 Full GC 原因，最后回到代码、流量和 JVM 参数上处理根因。

## 1. 启动演示环境

```bash
./scripts/lab-up jvm-gc
```

这个模式只启动：

```text
nacos
rabbitmq
kafka
gateway-service
mq-demo-bff-service
message-service
prometheus
grafana
```

不会启动 Kafka UI、Seata、完整酒店下单链路和前端，适合小内存云服务器。

访问入口：

```text
Gateway:    http://127.0.0.1:8080
Grafana:    http://127.0.0.1:3000  admin/admin
Prometheus: http://127.0.0.1:9091
Nacos:      http://127.0.0.1:8848/nacos
```

Grafana dashboard：

```text
Hotel Demo / Hotel Demo G1 Full GC Troubleshooting
```

## 2. 演示接口

所有接口都从 Gateway 进入：

```text
Gateway -> mq-demo-bff-service -> message-service
```

查看当前 JVM 状态：

```bash
curl -s http://127.0.0.1:8080/api/mq-demo/jvm-gc/status
```

制造短命对象，主要观察 Young GC、Eden 波动和分配速率：

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/jvm-gc/allocate-young?objects=2000&sizeKb=32'
```

保留大对象，主要观察 G1 humongous/old 区压力：

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/jvm-gc/retain-humongous?objects=8&sizeMb=2'
```

显式触发 GC，用来观察 `System.gc()` 这类人为 Full GC：

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/jvm-gc/explicit-gc?times=1'
```

清理演示接口保留的对象：

```bash
curl -s -XPOST http://127.0.0.1:8080/api/mq-demo/jvm-gc/clear
```

查看 GC 日志：

```bash
docker exec hotel-demo-message sh -c 'tail -n 120 /tmp/message-service-g1-gc.log'
```

## 3. 先判断是不是 Full GC

在 GC 日志里先找这些关键词：

```text
Pause Full
G1 Compaction Pause
Full GC
System.gc()
Metadata GC Threshold
Humongous Allocation
To-space exhausted
Evacuation Failure
```

G1 的普通 Young GC、Concurrent Mark、Mixed GC 不等于 Full GC。排查时不要只看到停顿就下结论，先确认日志里有没有 `Pause Full` 或 `G1 Compaction Pause`。

## 4. Grafana 看哪些图

在 `Hotel Demo G1 Full GC Troubleshooting` dashboard 中，优先看：

```text
Prometheus Target Up
```

如果 `message-service` 不是 1，先查服务和 `/actuator/prometheus`，不要继续分析空数据。

```text
Heap Usage
```

堆使用率长期高于 85%，且 GC 后降不下来，通常说明老年代里有大量存活对象，优先怀疑缓存、集合、队列、静态引用或消费积压。

```text
GC Pause Time Rate
GC Count by Cause and Action
Last 5m GC Cause Table
```

这里看 GC 是否变密、停顿是否变长，以及 `cause/action` 是否出现 `System.gc()`、`G1 Humongous Allocation` 等线索。

```text
Heap Pools
```

看 Eden、Survivor、Old Gen 的变化。Eden 抖动很正常；Old Gen 持续上升且不回落才危险。

```text
Allocation and Promotion
```

allocation 高说明分配快；promotion 高说明对象活过 Young GC 后进入老年代，可能是生命周期过长或 Survivor/新生代配置不合适。

```text
Live Data and Demo Retained Bytes
```

`live data` 接近 `max data` 时，G1 可回收空间不足，Full GC 风险升高。`demo retained` 是本演示接口保留的对象大小，用来和堆趋势对照。

## 5. 常见根因和证据

### 5.1 老年代真实占满

现象：

```text
Heap Usage 长期高
Full GC 后 heap used 下降很少
Old Gen 持续上升
live data 接近 max data
```

常见原因：

```text
内存泄漏
无界 Map/List/缓存
本地队列堆积
大批量查询一次性加载太多对象
消息消费者处理慢导致内存中堆积
```

处理：

```text
先抓 heap dump
用 MAT/JProfiler/YourKit 看 dominator tree
找 Retained Heap 最大的业务类型
给缓存和队列加上限
分页、流式处理或拆批
```

### 5.2 Humongous 对象过多

G1 中，超过 region 一半的大对象会走 humongous 分配。大对象频繁创建或长期保留，容易造成 region 碎片和 Full GC。

现象：

```text
GC 日志出现 Humongous
Old/Humongous 相关占用上升
Full GC 前经常有大对象分配
```

常见原因：

```text
超大 byte[]
大 JSON 字符串
大文件一次性读入内存
超大集合或聚合结果
```

处理：

```text
避免一次性把大文件/大响应读入内存
改成流式处理
限制请求体、分页大小和批处理大小
必要时调整 -XX:G1HeapRegionSize，但优先改对象大小和生命周期
```

### 5.3 显式 GC

现象：

```text
GC 日志 cause 是 System.gc()
业务流量不高但 Full GC 很规律
```

常见来源：

```text
代码里直接调用 System.gc()
第三方库调用 System.gc()
jcmd GC.run
RMI DGC 周期性触发
```

处理：

```text
搜索代码和依赖调用点
确认运维脚本是否执行 jcmd GC.run
必要时加 -XX:+DisableExplicitGC
```

### 5.4 晋升过快或疏散失败

现象：

```text
promotion rate 高
GC 日志出现 To-space exhausted 或 Evacuation Failure
Mixed GC 跟不上
很快进入 Full GC
```

常见原因：

```text
对象生命周期比预期长
瞬时流量过大
堆太小
G1 预留空间不足
并发标记启动太晚
```

处理：

```text
先降低单机流量或扩大堆验证
检查批量接口和消息消费并发
调低 -XX:InitiatingHeapOccupancyPercent 让并发标记更早开始
适当提高 -XX:G1ReservePercent
```

### 5.5 Metaspace 或类加载问题

现象：

```text
GC 日志出现 Metadata GC Threshold
nonheap/metaspace 持续上升
频繁动态生成类
```

常见原因：

```text
频繁创建 ClassLoader
动态代理/字节码生成失控
热部署或脚本引擎泄漏
```

处理：

```text
jcmd <pid> VM.classloader_stats
jcmd <pid> GC.class_histogram
检查动态代理、脚本、插件化加载逻辑
设置合理 MaxMetaspaceSize 并修复 ClassLoader 泄漏
```

## 6. 推荐排查顺序

1. 看告警和现象：Full GC 次数、单次停顿、发生时间、是否影响接口延迟。
2. Grafana 看趋势：heap 是否高位不降、Old Gen 是否持续上升、promotion 是否异常。
3. GC 日志确认原因：重点看 `Pause Full` 附近的 cause 和前后几次 GC。
4. 判断类型：老年代占满、humongous、显式 GC、晋升失败、metaspace。
5. 留证据：保存 GC 日志、Prometheus 查询截图、heap dump、问题时间段请求量。
6. 做最小验证：清缓存、降批量、停某类流量、扩大堆或调整参数，只改一个变量观察结果。
7. 修根因：修对象生命周期、缓存上限、批处理大小、流式处理、ClassLoader 泄漏等。

## 7. 常用命令

查看 JVM 进程：

```bash
docker exec hotel-demo-message jcmd
```

查看堆概况：

```bash
docker exec hotel-demo-message sh -c 'jcmd 1 GC.heap_info'
```

触发类直方图：

```bash
docker exec hotel-demo-message sh -c 'jcmd 1 GC.class_histogram | head -n 40'
```

生成 heap dump：

```bash
docker exec hotel-demo-message sh -c 'jcmd 1 GC.heap_dump /tmp/message-service.hprof'
```

从容器复制 heap dump 到服务器当前目录：

```bash
docker cp hotel-demo-message:/tmp/message-service.hprof ./message-service.hprof
```

查询 Prometheus 中 GC cause：

```bash
curl -G 'http://127.0.0.1:9091/api/v1/query' \
  --data-urlencode 'query=sum by (application,gc,cause,action) (increase(jvm_gc_pause_seconds_count{application="message-service"}[5m]))'
```

查询堆使用率：

```bash
curl -G 'http://127.0.0.1:9091/api/v1/query' \
  --data-urlencode 'query=100 * sum(jvm_memory_used_bytes{application="message-service",area="heap"}) / sum(jvm_memory_max_bytes{application="message-service",area="heap"})'
```

## 8. 面试回答模板

可以按这个结构回答：

```text
我会先确认是不是 Full GC，而不是把所有停顿都当成 Full GC。
第一步看监控：Full GC 频率、停顿时间、heap 使用率、Old Gen、promotion、allocation。
第二步看 GC 日志：定位 Pause Full 前后的 cause，例如 System.gc、humongous allocation、to-space exhausted、metadata threshold。
第三步按原因分流：
如果 Full GC 后 heap 降不下来，优先抓 heap dump 查内存泄漏或无界缓存；
如果 humongous 多，检查大 byte[]、大字符串、大 JSON、大文件和批量查询；
如果是 System.gc，排查代码、第三方库和运维命令，必要时禁用显式 GC；
如果是 evacuation failure/to-space exhausted，说明 G1 回收跟不上，要看晋升速率、堆大小、IHOP、ReservePercent 和流量峰值；
如果是 metadata threshold，要查 Metaspace 和 ClassLoader 泄漏。
最后我会用一次只改一个变量的方式验证，避免把参数调优当成根因修复。
```

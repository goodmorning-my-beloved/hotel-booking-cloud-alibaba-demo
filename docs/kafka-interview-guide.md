# Kafka 面试主线：结合当前工程覆盖 85% 高频场景

这份文档不是 API 清单，而是一套回答框架。面试时先说明结论，再讲原理、配置和当前工程证据，最后主动交代边界。

## 一、统一心智模型

Kafka 是按 partition 分片、支持副本复制的分布式追加日志：

```text
producer
  -> 根据显式 partition 或 key 选择 topic-partition
  -> 在客户端按 topic-partition 合批、压缩
  -> 写 partition leader
  -> follower 从 leader 拉取复制

consumer group
  -> group coordinator 协调成员
  -> assignor 把 partition 分给成员
  -> consumer 主动 poll
  -> 处理成功后提交“下一条要读取的位置”
```

四个最重要的坐标：

- topic：业务数据流的逻辑名称。
- partition：物理有序日志和并行度单位。
- offset：partition 内的位置，不是全局消息 ID。
- consumer group：一套独立的消费进度和分区分配。

## 二、生产者高频题

### 1. 一条消息怎样发送？

当前工程的 `KafkaDemoPublisherAdapter` 调用 `KafkaTemplate.send(topic, key, value)`：

1. key/value serializer 产生字节。
2. partitioner 选择 partition。
3. record 进入 producer 的 accumulator。
4. 同一 topic-partition 的小 record 合成 batch。
5. sender 线程向 partition leader 发 ProduceRequest。
6. Broker 按 `acks` 返回结果。
7. 应用得到 `RecordMetadata(topic, partition, offset)`。

当前普通接口同步等待 ACK，便于教学；`async-batch` 接口先提交一批 future，才更接近生产中的异步批量模式。

### 2. `acks=0/1/all` 有什么区别？

- `0`：不等 Broker，应答最快但不知道是否成功。
- `1`：leader 本地写入后确认；leader 突然故障而 follower 尚未复制时可能丢。
- `all`：等待当前 ISR 中所有副本确认；还需要合理的副本数和 `min.insync.replicas`。

当前 Lab 是单 Broker、replication factor 1，所以 `acks=all` 也只有一个副本参与，不能证明高可用。

生产常见组合：

```text
replication.factor=3
min.insync.replicas=2
producer acks=all
unclean.leader.election.enable=false
```

### 3. 幂等 producer 解决什么？

`enable.idempotence=true` 使用 PID、producer epoch 和每分区 sequence number，让 Broker 去重同一 producer session 内的重试。

它不解决：

- 应用自己调用两次 `send`；
- producer 重启后应用重新发送同一业务消息；
- consumer 业务执行两次；
- Kafka 与数据库的原子性。

因此工程同时保留 producer idempotence 和数据库 consumer idempotence，两者不是替代关系。

### 4. 怎样保证顺序？

Kafka 只保证 partition 内按 offset 有序。

业务要求“同一房间订单有序”时：

- 使用稳定的 `roomId` 作为 key；
- 不要在处理中随意并发同一 partition 的记录；
- producer 开启幂等，并保持 `max.in.flight.requests.per.connection<=5`；
- 业务必须接受不同 key 之间没有全局顺序。

扩 partition 后，`hash(key) % partitionCount` 可能变化，同一 key 的新消息可能进入新 partition。因此分区扩容是容量操作，也可能影响跨扩容时刻的 key 顺序。

### 5. 为什么 Kafka 吞吐高？

- partition 是追加日志，磁盘访问模式友好；
- 依赖操作系统 page cache；
- producer/consumer 都以 batch 工作；
- batch 压缩减少网络与磁盘；
- partition 提供并行度；
- Broker 到 consumer 的文件传输可减少用户态复制；
- consumer 使用 pull，可以按自己的能力批量拉取。

调优必须看瓶颈，不能只背配置：

```text
producer: batch.size, linger.ms, compression.type, buffer.memory
broker:   disk, network, partition count, request queue
consumer: fetch.min.bytes, fetch.max.wait.ms, max.poll.records, processing time
```

## 三、Broker、partition 和副本

### 1. leader、follower、ISR

- 每个 partition 有一个 leader，对外处理读写。
- follower 从 leader 拉取日志。
- ISR 是当前与 leader 保持同步的一组副本。
- `acks=all` 等待的是 ISR，而不是配置中的所有历史 replica。

LEO 表示某副本日志末端，HW 表示已复制到足够副本、对普通 consumer 可见的高水位。面试时不要把 LEO、HW 和 consumer committed offset 混在一起。

### 2. partition 越多越好吗？

不是。更多 partition 提升并行度，但也增加：

- 文件、索引和内存元数据；
- leader 选举与恢复时间；
- controller 和 group rebalance 压力；
- producer/consumer 请求数量；
- 扩容和副本迁移成本。

估算时同时考虑目标吞吐、单 partition 基准吞吐、consumer 并行度、未来增长和故障恢复时间。

### 3. retention 与 compaction

- `cleanup.policy=delete`：按时间或大小删除旧 segment，与是否消费无关。
- `cleanup.policy=compact`：长期保留每个 key 的最新值。
- compact topic 中 value 为 null 的 record 是 tombstone，用于删除 key。

Kafka 消费成功不会像传统队列那样立即删除消息。

## 四、消费者高频题

### 1. consumer group 怎样工作？

同一 group 内，一个 partition 同一时刻只分给一个 consumer；一个 consumer 可以负责多个 partition。consumer 数量超过 partition 数量时，多出的 consumer 空闲。

不同 group 有独立分配和 committed offset，因此都会读取同一 topic 的数据。“组间广播”只是便于理解的说法，本质是独立订阅进度。

当前工程：

```text
hotel-kafka-demo-primary
hotel-kafka-demo-audit
```

它们各自收到一份消息。

### 2. 什么会触发 rebalance？

经典消费组中常见原因：

- consumer 加入或退出；
- session timeout，Broker 判定成员失联；
- 超过 `max.poll.interval.ms` 没有再次 poll；
- 订阅 topic 的 partition 数变化；
- 正则订阅匹配的 topic 变化。

rebalance 期间分区会重新分配，可能暂停消费并放大重复处理窗口。应让 poll 线程及时返回、控制单批处理时长、正确处理 revoke 时的 offset，并根据场景选择 cooperative assignor 或静态成员。

Kafka 4.x 新 consumer protocol 把更多协调逻辑移到 Broker，使用增量分配降低全组同步停顿。当前工程的 3.6 client 仍用于学习经典协议，面试时要知道两者差异。

### 3. `session.timeout.ms` 和 `max.poll.interval.ms`

- `session.timeout.ms`：heartbeat 多久没到，认为进程或网络失联。
- `max.poll.interval.ms`：业务多久没再次调用 poll，认为消费处理失去进展。
- `max.poll.records`：一次 poll 最多交给应用多少条，不直接限制底层 fetch 大小。

heartbeat 正常但业务处理太慢，仍可能触发 `max.poll.interval.ms` rebalance。

### 4. offset 提交语义

committed offset 是“下一条要读取的位置”。处理完 offset 9 后，通常提交 10。

- 先提交、后处理：崩溃可能丢业务处理，接近 at-most-once。
- 先处理、后提交：崩溃可能重复，属于 at-least-once。
- Kafka transaction：可把消费 offset 和下游 Kafka 写入放在同一事务，形成 Kafka 内部 EOS。

当前 listener 采用 `MANUAL_IMMEDIATE`，数据库事务成功后才 `acknowledge()`。

### 5. `auto.offset.reset=earliest` 是不是每次从头？

不是。它只在 group 没有有效 committed offset，或 offset 已因 retention 越界时生效。已有 committed offset 时仍从提交位置继续。

### 6. lag 怎么算、怎么排查？

```text
lag = log end offset - committed offset
```

lag 持续增长时依次检查：

1. producer 流量是否突增；
2. consumer 是否频繁 rebalance；
3. 单条处理是否变慢、下游 DB/API 是否阻塞；
4. 是否有 poison record 或无限重试；
5. consumer 实例与 partition 是否匹配；
6. GC、CPU、网络、磁盘是否异常；
7. committed offset 是否因为提交失败没有推进。

当前 pause/resume 实验可以稳定制造和消除 lag。

## 五、重复、丢失、重试和 DLT

### 1. 为什么会重复消费？

典型时间线：

```text
写数据库成功
-> 进程在提交 Kafka offset 前崩溃
-> 新 consumer 从旧 committed offset 重读
-> 相同业务再次执行
```

正确做法是业务幂等，而不是承诺 Kafka 永不重复。

当前工程使用 `message_id` 数据库主键，并把去重记录和业务投影放在同一事务。仅用“内存 Set”会在重启、多实例和内存增长场景失效。

### 2. 阻塞重试与 retry topic

阻塞重试：

- consumer 线程在当前记录上等待并重试；
- 顺序容易维持；
- 适合几十毫秒到数秒的瞬时故障；
- 长时间重试会阻塞该 partition。

retry topic：

- 失败消息转发到带延迟语义的其他 topic；
- 主 partition 可以继续；
- 适合分钟级、小时级重试；
- 会改变原 topic 的严格顺序，并增加 topic 与运维复杂度。

确定性的参数错误、反序列化错误不应重复多次，应直接 DLT。

### 3. poison message 为什么特殊？

普通 `JsonDeserializer` 在 `poll()` 期间失败，listener 尚未拿到 `ConsumerRecord`。如果没有 `ErrorHandlingDeserializer`，consumer 可能不断读取同一坏 offset。

当前工程让 `ErrorHandlingDeserializer` 把异常和原始字节放进 header，error handler 再将原始 payload 发往 DLT。DLT consumer 使用 `byte[]` 接收，保证坏 JSON 也能登记工单。

### 4. DLT 之后怎么办？

DLT 不是“问题消失”，而是故障隔离：

```text
DLT -> 持久化 incident -> 告警 -> 定位根因
    -> 修复数据或程序 -> 人工确认/重放 -> 审计关闭
```

必须避免：

- DLT listener 再失败后无限投回同一 DLT；
- 只打印日志然后提交 offset；
- 不保留原 topic/partition/offset、异常和原始 payload；
- 未经幂等保护直接重放。

## 六、事务与 Exactly Once

需要区分三个概念：

1. 幂等 producer：避免客户端协议重试在 log 中产生重复。
2. Kafka transaction：多 partition 写入，以及“消费 offset + Kafka 输出”原子提交。
3. 业务 exactly-once：数据库、支付、外部 API 等副作用只发生一次。

`read_committed` consumer 只读取已提交事务，跳过 aborted transaction；但 aborted record 已占用 offset，所以 offset 不要求连续对应可见业务消息。

Kafka transaction 不能直接让 MySQL 与 Kafka 原子提交。跨系统常见方案：

- 业务事务写 Outbox，后台投递 Kafka；
- CDC/Debezium 读取数据库日志发布；
- 消费端唯一键和状态机幂等；
- 对账、补偿、告警作为最后防线。

当前 transaction 接口演示 Kafka 内部原子写；RabbitMQ Lab 中的本地消息表可以用于理解 Outbox 模式，两者边界必须说清。

## 七、序列化、Schema 与安全

JSON 简单但协议治理弱，生产需要：

- 显式 `schemaVersion` 或事件版本；
- 新增可选字段保持向后兼容；
- 不随意删除或改变字段语义；
- consumer 忽略未知字段，并对必需字段校验；
- 使用契约测试验证生产者与消费者。

大规模多团队场景可使用 Avro/Protobuf + Schema Registry 管理 backward/forward/full compatibility。

安全常见层次：

- TLS：传输加密；
- SASL：身份认证；
- ACL：topic、group、transactional ID 授权；
- quota：限制用户或 client 的吞吐；
- 密钥由 Secret 管理，不写进仓库。

## 八、运维排障回答模板

### 消息积压

先看 lag 是全 topic 还是单 partition，再看 consumer group 状态、rebalance、处理耗时和下游依赖。热点 key 导致单 partition 积压时，简单增加 consumer 无效，需要重新设计 key 或拆分热点。

### 消息丢失

分别排查：

- producer：send future 是否真正成功，是否错误吞掉异常；
- Broker：acks、ISR、min ISR、副本和非正常 leader 选举；
- consumer：是否先提交后处理，异常是否被吞；
- 业务：数据库事务是否回滚，是否把“查不到”误判成 Kafka 丢失。

### 重复消息

检查 producer 应用级重复调用、重试、consumer offset 提交窗口、rebalance、超时，以及幂等表唯一键。不要只调 Kafka 参数而忽略业务幂等。

### 某些消息一直慢

检查 key 分布和单 partition lag。总 lag 不大也可能存在一个热点 partition。再检查大消息、压缩 CPU、下游慢调用和 GC。

## 九、用当前项目做 90 秒项目回答

> 我在酒店订单项目里用 Spring Cloud Stream 发布完整下单事件，同时在 message-service 用原生 Spring Kafka 做独立 Lab。topic 有三个 partition，稳定业务 key 保证同类事件进入同一 partition；primary 与 audit 两个 group 演示组内分摊、组间独立 offset。生产端开启 acks=all、幂等、批量和 LZ4，并能返回 Broker ACK 的 partition/offset。消费端关闭自动提交，数据库去重记录与订单投影同事务成功后再手动提交 offset。短暂异常有限阻塞重试，耗尽后进入 DLT；反序列化异常通过 ErrorHandlingDeserializer 保留原始 payload，DLT 再落工单。项目还演示 pause 后 lag 增长、Kafka transaction 提交和回滚，以及 read_committed 隔离。默认单 Broker 是为了云服务器轻量运行，所以我不会把它描述成生产高可用；生产还需要三副本、min ISR、监控、Schema 和安全治理。

这段回答包含架构、可靠性、幂等、重试、可观测性、事务和边界，比单纯背参数更容易经受追问。

## 十、自测题

能够不看答案讲清下面问题，基础面覆盖通常已经足够：

1. topic、partition、offset、consumer group 分别是什么？
2. 为什么 Kafka 只保证 partition 内顺序？
3. 扩 partition 为什么可能影响 key 顺序？
4. `acks=all` 为什么不等于绝对不丢？
5. ISR、HW、LEO 分别是什么？
6. producer idempotence 如何工作，不能解决什么？
7. batch、linger、compression 如何共同影响吞吐和延迟？
8. consumer 多于 partition 会怎样？
9. 哪些情况触发 rebalance？
10. heartbeat 正常为什么仍可能被移出 group？
11. committed offset 为什么是下一条的位置？
12. `earliest` 为什么不是每次从头消费？
13. at-most-once、at-least-once、Kafka EOS 的区别？
14. 数据库成功、offset 失败时如何避免重复副作用？
15. 阻塞重试和 retry topic 如何选择？
16. poison message 为什么普通 listener 捕获不到？
17. DLT 为什么还需要工单、告警和补偿？
18. Kafka transaction 能否解决 MySQL 双写？
19. lag 持续增长如何系统排查？
20. partition 是不是越多越好？
21. retention delete 和 log compaction 有什么区别？
22. 如何治理 JSON/Avro/Protobuf Schema 兼容？
23. KRaft controller 负责什么？
24. 单 Broker Lab 与生产集群的差距有哪些？
25. Kafka 4.x 新 consumer protocol 改善了什么？

# Kafka 快速使用手册

这份手册配合当前工程的 `Kafka Lab` 使用。目标不是覆盖 Kafka 所有细节，而是帮助新同学通过接口和控制台命令快速理解市面上常问的 Kafka 问题。

## 代码位置

Kafka demo 下游能力严格放在 `message-service` 的 DDD 分层里，外部演示入口走 `mq-demo-bff-service`：

```text
mq-demo-bff-service
  interfaces/rest/MqDemoController.java
    Gateway 后面的 HTTP 演示入口
  application/service/MqDemoFacadeService.java
    BFF 编排
  application/port/out/MessageDemoGateway.java
  infrastructure/client/MessageServiceClient.java
    Feign 调用 message-service

message-service
  interfaces/rest/MessageController.java
    下游消息领域服务入口
  application/service/KafkaDemoApplicationService.java
    Kafka 学习用例编排
  application/service/KafkaDemoMetrics.java
    内存观测状态
  application/port/out/KafkaDemoMessagePublisher.java
  application/port/out/KafkaDemoInspector.java
  application/port/out/KafkaDemoTopology.java
    应用层出站端口
  domain/model/KafkaDemoMessage.java
  domain/model/KafkaDemoConsumedEvent.java
  domain/model/KafkaDemoTopicInfo.java
  domain/model/KafkaDemoConsumerLag.java
    领域对象和观测模型
  infrastructure/config/KafkaDemoConfig.java
    topic、producer、consumer、retry、DLT 配置
  infrastructure/messaging/KafkaDemoPublisherAdapter.java
  infrastructure/messaging/KafkaDemoConsumerAdapter.java
  infrastructure/messaging/KafkaDemoInspectorAdapter.java
    Spring Kafka 适配器
```

请求链路是 `gateway-service -> mq-demo-bff-service -> message-service`。新接口仍从 `interfaces/rest` 进入，业务编排写在 `application/service`，KafkaTemplate、AdminClient、@KafkaListener 等技术细节只能放在 `infrastructure`。

## 启动

在项目根目录执行：

```bash
./scripts/lab-up kafka
```

这个 lab 启动：

```text
Nacos + RabbitMQ + Kafka + gateway-service + mq-demo-bff-service + message-service
```

不启动订单、支付、Seata、Sentinel、Kafka UI 和前端构建。RabbitMQ 只是为了满足 `message-service` 当前已有 RabbitMQ demo bean 的运行前提。

## 演示接口

所有接口都走 Gateway：

```text
http://127.0.0.1:8080/api/mq-demo
```

MQ 连接、binder、topic、consumer group、RabbitMQ exchange/queue/routing key 等配置由启动脚本发布到 Nacos：

```text
namespace: pro
group: DEFAULT_GROUP
dataId: message-service.yaml
dataId: order-service.yaml
dataId: mq-demo-bff-service.yaml
```

### 1. 普通生产和 Broker ACK

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/normal?key=room-101'
```

返回里的 `topic`、`partition`、`offset` 来自 Broker ACK 后的 `RecordMetadata`。面试回答时可以说：生产者发送成功不只是本地方法返回，而是 Broker 确认后拿到消息落点。

### 2. key、partition 和分区内有序

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/key-order?key=room-101&count=5'
```

同一个 key 会被分配到同一个 partition；Kafka 只保证同一个 partition 内按 offset 有序，不保证整个 topic 全局有序。

如果业务要求“同一个房间的订单事件有序”，就应该把 `roomId` 或稳定业务键作为 message key。

### 3. consumer group

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/group?count=9'
```

当前代码有两个 group：

```text
hotel-kafka-demo-primary
hotel-kafka-demo-audit
```

同一个 group 内多个 consumer 分摊 partition；不同 group 会各自收到一份消息，并维护自己的 offset。这就是 Kafka 里“组内负载均衡，组间广播”的常用说法。

当前接口会按 Kafka 默认 Murmur2 分区算法动态寻找分别落入 0、1、2 分区的 key，避免固定示例 key 恰好发生哈希碰撞、看不到全部 partition。

### 4. 异步批量、linger 和压缩

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/async-batch?count=20'
```

普通接口为了明确展示每条消息的 Broker ACK，会同步等待 `RecordMetadata`。这个接口先快速提交全部发送请求，再统一等待结果，让 producer 有机会把同一 topic-partition 的记录合批。

当前配置同时展示：

```text
batch.size=32768
linger.ms=5
compression.type=lz4
delivery.timeout.ms=120000
```

面试时要说明：`linger.ms` 是允许等待合批的上限，不代表每条消息固定延迟这么久；batch 满了会提前发送。压缩发生在 batch 维度，通常能减少网络和磁盘，但会增加 CPU。

### 5. 重复消息和数据库幂等

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/duplicate?messageId=KMSG-DEMO-DUP'
```

Kafka 默认语义通常按 at-least-once 理解：消费者可能重复收到消息。因此消费者不能只靠 MQ 保证业务只执行一次，要用 `messageId`、业务唯一键或去重表做幂等。

当前 demo 不再用内存 Set，而是使用：

```text
kafka_demo_processed_message(message_id primary key)
kafka_demo_order_projection
```

去重记录和模拟业务投影在同一个 H2 本地事务中提交，成功后消费者才提交 Kafka offset。服务重启后去重记录仍然存在。

生产环境应该把它们换成业务 MySQL 表，并优先使用数据库唯一约束。仅用 Redis `SETNX` 时必须额外考虑 Redis 成功但数据库事务失败的状态不一致。

### 6. 失败重试和 DLT

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/dead-letter?key=room-102'
sleep 3
curl -s http://127.0.0.1:8080/api/mq-demo/kafka/status
```

消费者遇到 `fail=true` 会抛异常，`DefaultErrorHandler` 先重试，重试耗尽后通过 `DeadLetterPublishingRecoverer` 写入：

```text
hotel.kafka.demo.orders.DLT
```

生产里 DLT 后面通常还要接告警、人工排障、补偿任务或 parking-lot topic。

当前 `retry-max-retries=2` 表示“首次消费失败后再重试两次”，所以总投递次数是 3。这里使用阻塞重试，只适合短暂故障；分钟级或小时级重试应使用 retry topic，避免长期占住消费线程和 partition。

DLT consumer 使用 `ByteArrayDeserializer` 接收原始 payload，并先写入 `kafka_demo_dlt_incident` 工单表，再提交 DLT offset。这样即使 payload 已经损坏，也不会在 DLT 上再次反序列化失败形成循环。

### 7. poison message：反序列化失败

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/poison?key=room-103'
sleep 2
curl -s http://127.0.0.1:8080/api/mq-demo/kafka/status
```

接口故意发送非法 JSON。主消费者使用 `ErrorHandlingDeserializer`，因此能保留原始字节和异常头，并交给 error handler 直接发布到 DLT。反序列化异常属于确定性失败，默认不进行无意义重试。

如果直接使用 `JsonDeserializer`，异常发生在 `poll()` 返回记录之前，业务 listener 根本收不到消息，很容易在同一 offset 上反复报错。

### 8. Kafka 事务和 read_committed

提交事务：

```bash
curl -s -XPOST \
  'http://127.0.0.1:8080/api/mq-demo/kafka/transaction?failAfterFirst=false'
```

中途失败并回滚：

```bash
curl -s -XPOST \
  'http://127.0.0.1:8080/api/mq-demo/kafka/transaction?failAfterFirst=true'
```

每次事务尝试发送两条消息。成功时两条同时对 `read_committed` consumer 可见；失败时已经获得 offset 的记录也属于 aborted transaction，消费者两条都看不到。

这只证明 Kafka 内部原子写，不表示“Kafka + MySQL”自动成为一个分布式事务。数据库一致性仍然应该使用 Outbox/CDC 或业务幂等。

### 9. 暂停消费并观察 lag

```bash
curl -s -XPOST http://127.0.0.1:8080/api/mq-demo/kafka/consumer/pause
curl -s -XPOST 'http://127.0.0.1:8080/api/mq-demo/kafka/async-batch?count=20'
curl -s http://127.0.0.1:8080/api/mq-demo/kafka/status
curl -s -XPOST http://127.0.0.1:8080/api/mq-demo/kafka/consumer/resume
sleep 2
curl -s http://127.0.0.1:8080/api/mq-demo/kafka/status
```

pause 后 consumer 仍维持组成员身份和 heartbeat，但停止拉取业务记录，primary group lag 会增长；resume 后继续从 committed offset 消费，lag 最终回到 0。pause 本身通常不会触发 rebalance。

### 10. 状态、offset 和 lag

```bash
curl -s http://127.0.0.1:8080/api/mq-demo/kafka/status
```

重点看这些字段：

```text
topics
consumerLags
recentEvents
processedMessageIds
dltIncidents
primaryPauseRequested
transactionsCommitted
transactionsAborted
```

`consumerLags` 里：

```text
lag = partition 最新 offset - group 已提交 offset
```

如果 lag 持续增长，说明消费速度跟不上生产速度，可能需要扩 partition、扩 consumer 实例、优化消费逻辑或处理下游瓶颈。

注意 committed offset 表示“下一条将要消费的位置”。如果已经处理了 offset 0 到 9，正常 committed offset 是 10；end offset 也是 10 时，lag 为 0。

## CLI 观察命令

查看 topic 和 partition：

```bash
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic hotel.kafka.demo.orders
```

查看 consumer group offset 和 lag：

```bash
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group hotel-kafka-demo-primary
```

查看 DLT：

```bash
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic hotel.kafka.demo.orders.DLT
```

## 常见问题速答

### Kafka 为什么吞吐高？

核心原因：

- 顺序追加写日志，磁盘 IO 模式友好。
- partition 可并行读写。
- producer 支持批量发送和压缩。
- consumer 按 offset 顺序拉取，协议简单。
- page cache 和零拷贝减少数据复制成本。

### Kafka 能保证消息不丢吗？

需要生产、Broker、消费三侧一起配置：

- producer：`acks=all`、合理 `retries`、开启幂等生产者。
- broker：多副本，`min.insync.replicas` 不要过低。
- consumer：业务处理成功后再提交 offset。

当前 demo 是单 Broker、单副本，只适合学习 API，不是高可用生产配置。

### Kafka 能保证顺序吗？

Kafka 只能保证单 partition 内有序。要让同一类业务事件有序，就让它们使用同一个 key 进入同一个 partition。不要承诺 topic 全局有序，除非 topic 只有一个 partition，但这会牺牲并发能力。

### 消费失败怎么办？

常见做法：

- 可短暂恢复的异常：有限次数重试。
- 重试耗尽：写 DLT。
- DLT 后：告警、人工排障、补偿任务、parking-lot topic。

不要无限重试卡住整个 partition，也不要直接吞掉异常导致消息看似成功。

### 重复消费怎么办？

Kafka 常见消费语义是 at-least-once。重复消费是工程上必须处理的情况。解决方式：

- 消息带全局唯一 `messageId`。
- 业务表加唯一约束。
- 消费记录表按 `messageId` 去重。
- Redis `SETNX` 做短期幂等窗口。
- 业务状态机保证重复操作无副作用。

### partition 越多越好吗？

不是。partition 提升并行度，但也会增加文件句柄、元数据、leader 选举和 rebalance 成本。一般按目标吞吐、消费者并行度和未来扩容预估设计，避免为了“看起来大”盲目设置很高。

### consumer 数量越多越好吗？

同一个 group 内，最多只有 partition 数量个 consumer 能同时消费同一个 topic。consumer 数量超过 partition 数量，多出来的 consumer 会空闲。

### offset 是什么？

offset 是某个 partition 内消息的位置。consumer group 提交 offset 表示“这个 group 消费到哪里了”。不同 group 的 offset 互不影响。

### lag 是什么？

lag 是最新消息位置和 group 已提交位置之间的差距。它反映消费积压，不等于错误，但持续增长需要排查生产速度、消费耗时、下游依赖和 consumer 实例数。

## 面试覆盖矩阵

当前轻量 Lab 可以直接结合代码回答以下主题：

| 主题 | 对应代码/实验 |
| --- | --- |
| topic、partition、offset、key | normal、key-order、group |
| 分区内有序、哈希分区、扩分区影响 | key-order、`keyForPartition` |
| 组内负载均衡、组间独立消费 | primary/audit 两个 group |
| producer ACK、幂等、重试时间边界 | normal 与 producer 配置 |
| batch、linger、压缩 | async-batch |
| 手动提交、at-least-once、lag | consumer 与 pause/resume |
| 数据库幂等 | duplicate、两张 H2 表 |
| 阻塞重试、异常分类、DLT | dead-letter |
| 反序列化 poison message | poison、`ErrorHandlingDeserializer` |
| DLT 工单和人工闭环 | `kafka_demo_dlt_incident` |
| Kafka transaction、read_committed | transaction |
| AdminClient、topic 配置、group offset | status |
| KRaft、Broker/Controller 角色 | docker-compose Kafka 配置 |
| Spring Cloud Stream 与 Spring Kafka | 完整下单链路与独立 Lab 对照 |

以下主题以面试知识和设计说明为主，不应该伪装成当前单 Broker Lab 已经证明：

- 三 Broker 副本故障、leader 选举、ISR 收缩、`min.insync.replicas`；
- 生产级容量压测和精确吞吐调优；
- Schema Registry、Avro/Protobuf 的兼容性策略；
- SASL/SSL、ACL、多租户配额；
- Kafka Streams 状态存储和 changelog；
- Kafka 4.x 新 consumer protocol 与 share group。

这些内容见 [kafka-interview-guide.md](kafka-interview-guide.md)。默认 Lab 保持单 Broker，是为了让云服务器能稳定运行；不能拿它证明生产高可用。

## 停止

```bash
./scripts/lab-down
```

# 后端组件实验室

这个项目已经从“默认启动完整酒店预订系统”调整为“后端组件实验室”学习模式。默认不要启动前端，也不要启动完整酒店业务链路；一次只启动一个 lab，用最少服务学习一个后端组件。

保留原有酒店业务代码，方便以后回看 Gateway、Nacos、RabbitMQ、Kafka、Sentinel、Seata 在完整链路里的组合方式。但日常学习请优先使用下面的新入口。

酒店主链路已按轻量 DDD + 端口适配器结构组织，并带有领域、用例和架构测试。新增接口、领域规则、Feign/MQ/DB 实现时，先看 [docs/ddd-development-guide.md](docs/ddd-development-guide.md)。学习 Kafka 时看 [docs/kafka-quick-start-guide.md](docs/kafka-quick-start-guide.md)。

## 新入口

```bash
cd /opt/codex-runner/workspace/hotel-booking-sca-demo

./scripts/lab-up nacos
./scripts/lab-up gateway
./scripts/lab-up rabbitmq
./scripts/lab-up kafka
./scripts/lab-up sentinel
./scripts/lab-up sentinel-chain
./scripts/lab-up seata

./scripts/lab-status
./scripts/lab-down
```

`lab-up` 每次启动新 lab 前都会自动执行 `docker compose down --remove-orphans`，避免多个组件实验同时堆在 4GB 云服务器上。

## Lab 总览

| Lab | 启动服务 | 学什么 | 预计资源 |
| --- | --- | --- | --- |
| `nacos` | Nacos + user-service + gateway-service | 服务注册、服务发现、provider/consumer | 中 |
| `gateway` | Nacos + gateway-service + user-service | 路由、`StripPrefix`、`lb://` 负载均衡路由 | 中 |
| `rabbitmq` | Nacos + RabbitMQ + gateway-service + message-service | queue、exchange、routing key、发布与消费 | 中 |
| `kafka` | Nacos + RabbitMQ + Kafka + gateway-service + message-service | topic、partition、producer ACK、consumer group、offset、lag、DLT | 中偏高 |
| `sentinel` | Nacos + Sentinel Dashboard + user-service | 资源名、限流规则、blockHandler | 中 |
| `sentinel-chain` | Nacos + Sentinel Dashboard + Prometheus + Grafana + Gateway + BFF + A + B | Sentinel 集群流控、Prometheus 指标采集、Grafana JVM/线程面板 | 中偏高 |
| `seata` | Nacos + Seata Server + hotel-service + payment-service | Seata 控制台、客户端配置、事务组映射 | 中偏高 |
| `full` | 全部中间件 + 6 个 Java 服务 + Kafka UI + 前端静态页 | 旧酒店预订完整链路 | 高 |

4GB 机器上不建议同时运行 Kafka、Seata、Sentinel、完整 Java 链路。默认也不会启动 Kafka UI、完整前端或完整酒店链路。

## Nacos Lab

启动：

```bash
./scripts/lab-up nacos
```

学习重点：

- `user-service` 作为 provider 注册到 Nacos。
- `gateway-service` 作为 consumer，通过 Nacos 找到 `user-service`。
- 在 Nacos 控制台观察服务列表和实例。

验证：

```bash
curl -s http://127.0.0.1:8081/users/1
curl -s http://127.0.0.1:8080/api/users/1
```

控制台：

```text
http://127.0.0.1:8848/nacos
```

停止：

```bash
./scripts/lab-down
```

## Gateway Lab

启动：

```bash
./scripts/lab-up gateway
```

学习重点：

- 看 [gateway-service/src/main/resources/application.yml](gateway-service/src/main/resources/application.yml) 里的 routes。
- `/api/users/**` 被 Gateway 转发到 `lb://user-service`。
- `StripPrefix=1` 会把 `/api/users/1` 转成后端的 `/users/1`。

验证：

```bash
curl -s http://127.0.0.1:8080/api/users/1
curl -s http://127.0.0.1:8080/actuator/gateway/routes
```

停止：

```bash
./scripts/lab-down
```

## RabbitMQ Lab

启动：

```bash
./scripts/lab-up rabbitmq
```

学习重点：

- exchange 负责按 routing key 分发消息，queue 是消息暂存地。
- direct exchange 按 routing key 精确匹配，适合明确的一类业务事件。
- topic exchange 支持通配符：`*` 匹配一个单词，`#` 匹配零个或多个单词，适合按事件层级订阅。
- fanout exchange 忽略 routing key，把同一条消息广播复制到所有绑定队列，适合一发多下游通知。
- 生产端先写本地消息表 `mq_outbox_message`，再异步发送 RabbitMQ，避免“业务成功但消息没有记录”。
- publisher confirm 使用异步回调更新本地消息表：Broker ack 后标记 `SENT`，nack 或超时后进入 `RETRYING`。
- 定时重试任务会扫描 `RETRYING` 和 confirm 超时的消息，按 `attempt_count` 做有限次数补偿。
- mandatory return 发现“消息到达 exchange 但没有路由到 queue”，本地消息表标记 `RETURNED`，这种通常要修 binding/routing key 后再人工补偿。
- exchange、queue、消息都使用持久化配置，消费者业务成功后才手动 ACK。
- 消费端用 `messageId` 做幂等，重复消息 ACK 但不重复执行业务。
- 消费失败时 `basicNack(requeue=false)`，消息进入死信队列。
- DLQ Listener 自动监听死信队列，把失败消息写入 `mq_dlq_incident`，状态为 `PENDING`。
- DLQ 原消息只有在 PENDING 补偿任务落库成功后才 ACK 删除；如果落库失败，就 NACK 重新入 DLQ。
- 人工排障或补偿后，调用 `POST /dlq/incidents/{incidentId}/resolve` 处理指定任务，任务状态变成 `RESOLVED`。

控制台：

```text
http://127.0.0.1:15672
guest / guest
```

验证：

```bash
curl -s -XPOST http://127.0.0.1:8080/api/messages/rabbitmq/demo/normal
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/rabbitmq/demo/duplicate?messageId=MSG-DEMO-1'
curl -s -XPOST http://127.0.0.1:8080/api/messages/rabbitmq/demo/dead-letter
curl -s 'http://127.0.0.1:8080/api/messages/dlq/incidents?status=PENDING'
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/dlq/incidents/INC-替换为上一步返回的incidentId/resolve?compensationNote=order%20status%20fixed%20manually'
curl -s -XPOST http://127.0.0.1:8080/api/messages/rabbitmq/demo/unroutable
curl -s -XPOST http://127.0.0.1:8080/api/messages/rabbitmq/demo/topic
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/rabbitmq/demo/topic?routingKey=hotel.booking.payment.timeout'
curl -s -XPOST http://127.0.0.1:8080/api/messages/rabbitmq/demo/fanout
curl -s http://127.0.0.1:8080/api/messages/rabbitmq/demo/status
```

控制台观察：

- `Exchanges` 里看 `hotel.booking.exchange`、`hotel.booking.dlx`、`hotel.booking.topic.exchange`、`hotel.booking.fanout.exchange`。
- `Queues and Streams` 里看 `hotel.booking.created.queue` 和 `hotel.booking.created.dlq`。
- topic 通配队列看 `hotel.booking.topic.single-word.queue` 和 `hotel.booking.topic.multi-word.queue`。
- fanout 广播队列看 `hotel.booking.fanout.sms.queue` 和 `hotel.booking.fanout.points.queue`。
- 触发 `/normal` 后，接口会先返回本地消息表的 `outboxId`，随后 `/status` 里的 `recentOutboxMessages` 会从 `WAIT_CONFIRM` 变成 `SENT`。
- 触发 `/dead-letter` 后，正常消费者会把消息 nack 到 DLQ，DLQ Listener 随后把它写成 `PENDING` 事故任务。
- 查询 `/dlq/incidents?status=PENDING` 可以拿到待补偿的 `incidentId`。
- 触发 `/dlq/incidents/{incidentId}/resolve` 后，服务模拟人工补偿成功并把这条任务标记为 `RESOLVED`。
- 触发 `/duplicate?messageId=MSG-DEMO-1` 后，接口状态里的 `duplicated` 会增加，但业务只处理一次。
- 触发 `/unroutable` 后，接口状态里的 `returned` 会增加，本地消息表对应记录会变成 `RETURNED`。
- 触发 `/topic` 默认 routing key 是 `hotel.booking.created`，会同时命中 `hotel.booking.*` 和 `hotel.booking.#` 两个队列。
- 触发 `/topic?routingKey=hotel.booking.payment.timeout` 时，只有 `hotel.booking.#` 队列会收到，因为 `*` 只能匹配一个单词。
- 触发 `/fanout` 后，短信队列和积分队列都会收到同一条消息，说明 fanout 是广播复制，不是多个消费者抢一条。

面试表达可以这样串起来：

```text
direct exchange 精确匹配 routing key；topic exchange 让消费者用 * 和 # 做事件模式订阅；fanout exchange 忽略 routing key，给每个绑定队列复制一份消息。
生产端可靠投递 = 本地消息表先落库 + 异步 publisher confirm 回写状态 + 定时任务补偿重试。
confirm 只证明 Broker 收到了消息；mandatory return 证明是否成功路由到队列。
重试可能带来重复投递，所以消费者必须用 messageId 做幂等，并在业务成功后手动 ACK。
DLQ 是失败现场，不是工单系统；DLQ Listener 先把死信落成 PENDING 补偿任务，任务落库成功后 ACK 掉 DLQ 原消息。
人工排障后按 incidentId resolve 指定任务，补偿成功标记 RESOLVED，失败标记 FAILED 等待再次处理或转 parking-lot。
```

停止：

```bash
./scripts/lab-down
```

## Kafka Lab

启动：

```bash
./scripts/lab-up kafka
```

学习重点：

- topic 是消息分类，partition 是 topic 内的并行日志。
- producer 写入 topic，Broker ACK 后返回 partition 和 offset。
- key 决定 partition，同一 key 在同一 partition 内有序。
- consumer group 内负载均衡，group 间各自消费一份。
- offset 表示消费进度，lag 表示消费积压。
- 重复消费要靠业务幂等，失败消息应有限重试后进 DLT。

这个 lab 不启动 Kafka UI、订单、支付、Seata、Sentinel 和前端构建，减少内存占用。RabbitMQ 只是为了满足 `message-service` 当前已有 RabbitMQ demo bean 的运行前提。

验证：

```bash
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/kafka/demo/normal?key=room-101'
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/kafka/demo/key-order?key=room-101&count=5'
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/kafka/demo/group?count=9'
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/kafka/demo/duplicate?messageId=KMSG-DEMO-DUP'
curl -s -XPOST 'http://127.0.0.1:8080/api/messages/kafka/demo/dead-letter?key=room-102'
sleep 3
curl -s http://127.0.0.1:8080/api/messages/kafka/demo/status
```

CLI 观察：

```bash
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic hotel.kafka.demo.orders
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic hotel.kafka.demo.orders --from-beginning --max-messages 1 --timeout-ms 5000
docker exec hotel-demo-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group hotel-kafka-demo-primary
```

快速手册：[docs/kafka-quick-start-guide.md](docs/kafka-quick-start-guide.md)。

停止：

```bash
./scripts/lab-down
```

## Sentinel Lab

启动：

```bash
./scripts/lab-up sentinel
```

学习重点：

- `user-service` 暴露了一个最小限流资源：`userSentinelLab`。
- 代码在 [user-service/src/main/java/com/example/hotel/user/interfaces/rest/UserController.java](user-service/src/main/java/com/example/hotel/user/interfaces/rest/UserController.java)。
- Dashboard 里可以给资源加 QPS 规则，触发 `blockHandler`。

控制台：

```text
http://127.0.0.1:8858
```

验证：

```bash
curl -s http://127.0.0.1:8081/users/lab/sentinel
```

在 Sentinel Dashboard 里找到 `user-service`，给资源 `userSentinelLab` 添加流控规则，QPS 阈值设为 `1`，然后快速多次执行上面的 curl。被限流时会返回：

```text
Sentinel blocked userSentinelLab
```

停止：

```bash
./scripts/lab-down
```

## Sentinel Chain Lab

启动：

```bash
./scripts/lab-up sentinel-chain
```

学习重点：

- 请求链路是 `Gateway -> sentinel-bff-service -> sentinel-a-service -> sentinel-b-service`。
- `sentinel-token-server` 独立部署，两个 BFF 实例、A 服务、B 服务都作为 cluster client 共享各自资源的集群 QPS。
- Nacos 使用 `pro` 命名空间，启动脚本会自动创建并把 Sentinel 规则发布到 `pro`。
- Gateway 使用 `spring-cloud-alibaba-sentinel-gateway`，路由资源是 `sentinel-chain-bff`。
- BFF 和 A 服务都开启 `feign.sentinel.enabled=true`，通过 Feign 调下游。
- 三个 Web 服务都用 `@SentinelResource` 暴露清晰资源名：`chainBffEntry`、`chainAWork`、`chainBWork`。
- Prometheus 自动抓取 Gateway、token server、两个 BFF、A、B 的 `/actuator/prometheus`。
- Grafana 自动加载 `Hotel Demo JVM and Thread Metrics` 面板，用来观察 JVM 线程数和 BFF 演示线程池指标。

公网入口：

```text
Sentinel Dashboard: http://111.230.36.77:8090
Nacos:              http://111.230.36.77:8848/nacos
Grafana:            http://111.230.36.77:3000  admin/admin
Prometheus:         http://111.230.36.77:9091
链路请求:           http://111.230.36.77:8080/api/lab-chain/entry
```

本机验证：

```bash
curl -s http://127.0.0.1:8080/api/lab-chain/entry
```

集群流控验证：

```bash
./scripts/demo-sentinel-cluster-flow.sh
```

Grafana 线程指标验证：

```bash
./scripts/demo-grafana-thread-metrics.sh
```

脚本会通过 Gateway 调用 BFF 的 `/lab-chain/thread-test`，让 BFF 内置演示线程池短暂变忙，然后查询 Prometheus 中的线程池饱和度、吞吐、拒绝、耗时和 JVM 线程指标。打开 Grafana 后进入：

```text
Hotel Demo / Hotel Demo JVM and Thread Metrics
```

Grafana 里的线程池排障面板会展示 Prometheus 当前能采集到的这些 BFF 演示线程池指标：

```text
Lab Thread Pool Saturation:
  executor_active_threads                当前正在执行任务的线程数
  executor_pool_size_threads             当前线程池线程数
  executor_pool_core_threads             核心线程数
  executor_pool_max_threads              最大线程数
  executor_queued_tasks                  当前排队任务数
  executor_queue_remaining_tasks         队列剩余容量

Lab Thread Throughput:
  rate(lab_thread_test_requests_total)   /thread-test 接口请求速率
  rate(lab_thread_test_tasks_total)      成功提交到线程池的任务速率
  rate(executor_completed_tasks_total)   线程池完成任务速率

Lab Thread Rejections:
  lab_thread_test_rejected_tasks_total   拒绝任务总数
  rate/increase(...)                     拒绝速率和最近 5 分钟拒绝量

Lab Thread Task Duration:
  lab_thread_test_task_duration_seconds_bucket  任务耗时直方图，用于 p95/p99
  lab_thread_test_task_duration_seconds_sum     任务耗时总和，用于平均耗时
  lab_thread_test_task_duration_seconds_count   已计时完成任务数
  lab_thread_test_task_duration_seconds_max     最近窗口最大耗时
```

`executor_*` 指标来自 Micrometer 标准 `ExecutorServiceMetrics` 绑定器，按 `name="lab-thread-pool"` 区分这个演示线程池。`lab_thread_test_*` 是 `/lab-chain/thread-test` 压测接口额外注册的业务计数器和任务耗时 Timer，用来观察请求、提交、拒绝和任务执行耗时。

采集链路是：BFF 通过 Spring Boot Actuator 在 `/actuator/prometheus` 暴露 Micrometer 指标；Prometheus 按 `monitoring/prometheus/prometheus.yml` 每 5 秒主动拉取各服务的这个端点；Grafana 的数据源指向 Prometheus，打开 dashboard 时由 Grafana 执行 PromQL 查询并展示图表。也就是说 Prometheus 不会把指标推送给 Grafana，Grafana 是从 Prometheus 查询数据。

在 Sentinel Dashboard 里先多请求几次链路接口，然后观察这些应用和资源：

```text
gateway-service:
  sentinel-chain-bff
  /api/lab-chain/**

sentinel-bff-service:
  chainBffEntry
  /lab-chain/entry
  sentinel-a-service GET /a/work

sentinel-a-service:
  chainAWork
  /a/work
  sentinel-b-service GET /b/work

sentinel-b-service:
  chainBWork
  /b/work
```

内置集群流控规则已经覆盖 `chainBffEntry`、`chainAWork`、`chainBWork`。也可以在 Nacos 的 `pro` 命名空间里修改对应 JSON，降低 QPS 阈值后快速刷新链路请求，观察 blockHandler 或 Feign fallback 的效果。

现在 `sentinel-chain` 也内置了 Nacos 动态规则演示。启动脚本会自动把规则发布到 Nacos，规则文件在：

```text
nacos-config/sentinel/
```

详细说明见：

```text
docs/sentinel-nacos-rules-and-fallbackfactory.md
docs/sentinel-cluster-flow-control.md
```

停止：

```bash
./scripts/lab-down
```

## Seata Lab

启动：

```bash
./scripts/lab-up seata
```

学习重点：

- Seata Server 如何启动。
- 客户端如何配置 `tx-service-group`。
- `hotel-service`、`payment-service` 如何作为最小 Seata 客户端接入。

控制台：

```text
Seata: http://127.0.0.1:7091
Nacos: http://127.0.0.1:8848/nacos
```

验证：

```bash
curl -s http://127.0.0.1:8082/actuator/health
curl -s http://127.0.0.1:8084/actuator/health
```

降级说明：

当前项目原本为了轻量学习没有引入 MySQL，业务数据是内存数据。因此 `seata` lab 默认不启动数据库、不创建 AT 模式 `undo_log`，也不跑真实数据库分布式事务。它是 4GB 机器上的 Seata 观察版：先学习 Seata Server、客户端配置和事务组映射。真正的 AT 事务实验建议后续单独加 MySQL + 两张测试表 + `undo_log`，不要和 Kafka、完整酒店链路一起跑。

停止：

```bash
./scripts/lab-down
```

## Full 模式

只有明确想看旧酒店预订完整链路时才使用：

```bash
./scripts/lab-up full
```

警告：`full` 会启动 Nacos、Sentinel Dashboard、RabbitMQ、Kafka、Kafka UI、Seata、Gateway、user/hotel/order/payment/message 等 Java 服务，并构建前端静态资源。它可能在 4GB 云服务器上产生较高内存占用和磁盘 IO，不适合作为默认学习入口。

Full 模式访问：

```text
Hotel Web: http://127.0.0.1:8080
Gateway:   http://127.0.0.1:8080/api/hotels
Nacos:     http://127.0.0.1:8848/nacos
RabbitMQ:  http://127.0.0.1:15672  guest/guest
Kafka UI:  http://127.0.0.1:8090
Seata:     http://127.0.0.1:7091
```

冒烟测试：

```bash
./scripts/smoke-test.sh
```

停止：

```bash
./scripts/lab-down
```

## 旧脚本说明

旧脚本仍保留，便于回退：

```bash
./scripts/start-all.sh
./scripts/start-infra.sh
./scripts/build-with-docker.sh
./scripts/build-frontend.sh
```

日常学习请优先使用 `./scripts/lab-up <lab>`。不要把 `start-all.sh` 当默认入口。

## 建议学习顺序

1. `nacos`：先看服务注册与发现，理解 provider/consumer。
2. `gateway`：再看统一入口如何按路径转发。
3. `rabbitmq`：理解 exchange、queue、routing key。
4. `kafka`：理解 topic、partition、offset、consumer。
5. `sentinel`：理解资源名、流控规则、限流后的 fallback。
6. `sentinel-chain`：看 Gateway、Web、Feign 三类 Sentinel 资源在面板上的样子。
7. `seata`：先看配置和控制台，再决定是否追加 MySQL 做真实 AT 事务。
8. `full`：最后再看完整酒店预订链路如何把这些组件串起来。

## 版本

```text
Spring Boot: 3.2.4
Spring Cloud: 2023.0.1
Spring Cloud Alibaba: 2023.0.1.0
Java: 17
```

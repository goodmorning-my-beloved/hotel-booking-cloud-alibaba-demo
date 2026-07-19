# Sentinel 独立 Token Server 集群流控演示

这个 demo 现在使用独立部署的 Sentinel 集群流控 token server。

拓扑是：

```text
sentinel-token-server:18730
        ^
        |
  +-----+-----+----------+----------+
  |           |          |          |
BFF 8086   BFF 8186   A 8087    B 8088
```

两个 BFF 实例都是 `sentinel-bff-service`，A 服务和 B 服务也作为 cluster client 连接独立 token server。`chainBffEntry`、`chainAWork`、`chainBWork` 的流控判断都不再由本机单独完成，而是由 token server 统一发令牌。

Nacos 使用 `pro` 命名空间，启动脚本会自动创建这个命名空间，并把 Sentinel 规则发布进去。

## 规则

Nacos 里的规则文件：

```text
nacos-config/sentinel/sentinel-bff-service-flow-rules.json
nacos-config/sentinel/sentinel-a-service-flow-rules.json
nacos-config/sentinel/sentinel-b-service-flow-rules.json
```

核心内容：

```json
{
  "resource": "chainBffEntry",
  "grade": 1,
  "count": 6,
  "clusterMode": true,
  "clusterConfig": {
    "flowId": 1001,
    "thresholdType": 1,
    "fallbackToLocalWhenFail": true
  }
}
```

含义：

```text
两个 BFF 实例加起来，chainBffEntry 总通过阈值是 6 QPS。
A 服务 chainAWork 总通过阈值是 4 QPS。
B 服务 chainBWork 总通过阈值是 2 QPS。
```

## 一键启动

```bash
./scripts/lab-up sentinel-chain
```

这个命令会启动：

```text
Nacos:                 8848
Sentinel Dashboard:    8858，本机代理 8090
sentinel-token-server: 8720 健康检查端口，18730 集群令牌端口
sentinel-bff-service:  8086
sentinel-bff-service-2:8186
sentinel-a-service:    8087
sentinel-b-service:    8088
gateway-service:       8080
Prometheus:            9091
Grafana:               3000
```

## 验证集群流控

直接跑演示脚本：

```bash
./scripts/demo-sentinel-cluster-flow.sh
```

脚本会分三段发送请求：

```text
BFF 阶段：平均打到 http://127.0.0.1:8086/lab-chain/entry 和 http://127.0.0.1:8186/lab-chain/entry
A 阶段：直打 http://127.0.0.1:8087/a/work
B 阶段：直打 http://127.0.0.1:8088/b/work
```

预期会看到类似统计：

```text
BFF phase:
  BFF blocked:          14

A phase:
  A service blocked:    16

B phase:
  B service blocked:    18
```

实际数字可能因为机器速度和时间窗口略有差异，但只要能看到：

```text
Sentinel blocked chainBffEntry: FlowException
Sentinel blocked chainAWork: FlowException
Sentinel blocked chainBWork: FlowException
```

就说明 BFF、A、B 的方法资源都被集群流控拦住了。

## 验证 Grafana JVM/线程指标

`sentinel-chain` 同时会启动 Prometheus 和 Grafana。

访问入口：

```text
Prometheus: http://127.0.0.1:9091
Grafana:    http://127.0.0.1:3000
账号密码:   admin / admin
```

Prometheus 会抓取这些服务的 `/actuator/prometheus`：

```text
gateway-service
sentinel-token-server
sentinel-bff-service
sentinel-bff-service-2
sentinel-a-service
sentinel-b-service
```

Grafana 会自动加载这个 dashboard：

```text
Hotel Demo / Hotel Demo JVM and Thread Metrics
```

可以先运行线程指标演示脚本：

```bash
./scripts/demo-grafana-thread-metrics.sh
```

脚本会调用 Gateway 路由：

```text
http://127.0.0.1:8080/api/lab-chain/thread-test?tasks=32&busyMs=8000
```

BFF 会把任务提交到一个 8 线程的演示线程池，Prometheus 随后能看到这些指标：

```text
lab_thread_pool_active_threads
lab_thread_pool_queue_size
lab_thread_test_tasks_total
jvm_threads_live_threads
jvm_threads_states_threads
```

如果看到 `lab_thread_pool_active_threads{application="sentinel-bff-service"}` 变成 8，说明 Grafana/Prometheus 和 BFF 自定义线程池指标已经打通。

## 为什么能证明是集群流控

如果是单机限流，两个 BFF 实例每个 6 QPS，总共最多可能通过 12 QPS。

现在规则是：

```text
clusterMode=true
thresholdType=1
count=6
```

两个 BFF 都向同一个独立 token server 申请令牌，所以 `chainBffEntry` 总通过量按 6 QPS 控制。A、B 服务虽然当前各启动一个实例，也同样走 token server；以后如果 A/B 横向扩容，多实例会天然共享同一份 `chainAWork`、`chainBWork` 集群阈值。

## 手工启动方式

先发布规则到 Nacos：

```bash
./scripts/publish-sentinel-rules-to-nacos.sh
```

启动独立 token server：

```bash
NACOS_NAMESPACE=pro \
SENTINEL_CLUSTER_NAMESPACES=sentinel-bff-service,sentinel-a-service,sentinel-b-service \
SENTINEL_CLUSTER_FLOW_RULE_DATA_IDS=sentinel-bff-service-flow-rules.json,sentinel-a-service-flow-rules.json,sentinel-b-service-flow-rules.json \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
mvn -pl sentinel-token-server spring-boot:run
```

启动第一个 BFF client：

```bash
SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
NACOS_NAMESPACE=pro \
mvn -pl sentinel-bff-service spring-boot:run \
  -Dspring-boot.run.arguments="--server.port=8086"
```

启动第二个 BFF client：

```bash
SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
NACOS_NAMESPACE=pro \
mvn -pl sentinel-bff-service spring-boot:run \
  -Dspring-boot.run.arguments="--server.port=8186"
```

A 服务和 B 服务手工启动时也要带上同样的 client 和 namespace 环境变量：

```bash
SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
NACOS_NAMESPACE=pro \
mvn -pl sentinel-a-service spring-boot:run

SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
NACOS_NAMESPACE=pro \
mvn -pl sentinel-b-service spring-boot:run
```

如果 token server 和 BFF 不在同一台机器，把 `SENTINEL_CLUSTER_SERVER_HOST` 改成 token server 的内网 IP。

## 关键点

`sentinel-token-server` 只做集群令牌服务，不处理业务请求。

`sentinel-bff-service`、`sentinel-a-service`、`sentinel-b-service` 都只通过：

```text
SENTINEL_CLUSTER_MODE=client
```

连接独立 token server。

# Sentinel 独立 Token Server 集群流控演示

这个 demo 现在使用独立部署的 Sentinel 集群流控 token server。

拓扑是：

```text
sentinel-token-server:18730
        ^
        |
  +-----+-----+
  |           |
BFF 8086   BFF 8186
```

两个 BFF 实例都是 `sentinel-bff-service`，都作为 cluster client 连接独立 token server。`chainBffEntry` 的流控判断不再由每个 BFF 本机单独完成，而是由 token server 统一发令牌。

## 规则

Nacos 里的规则文件：

```text
nacos-config/sentinel/sentinel-bff-service-flow-rules.json
```

核心内容：

```json
{
  "resource": "chainBffEntry",
  "grade": 1,
  "count": 3,
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
两个 BFF 实例加起来，chainBffEntry 总通过阈值是 3 QPS。
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
```

## 验证集群流控

直接跑演示脚本：

```bash
./scripts/demo-sentinel-cluster-flow.sh
```

脚本会把请求平均打到两个 BFF 实例：

```text
http://127.0.0.1:8086/lab-chain/entry
http://127.0.0.1:8186/lab-chain/entry
```

预期会看到类似统计：

```text
passed:  3
blocked: 17
```

实际数字可能因为机器速度和时间窗口略有差异，但只要能看到：

```text
Sentinel blocked chainBffEntry: FlowException
```

就说明 BFF 方法资源 `chainBffEntry` 被集群流控拦住了。

## 为什么能证明是集群流控

如果是单机限流，两个 BFF 实例每个 3 QPS，总共最多可能通过 6 QPS。

现在规则是：

```text
clusterMode=true
thresholdType=1
count=3
```

两个 BFF 都向同一个独立 token server 申请令牌，所以总通过量按 3 QPS 控制。

## 手工启动方式

先发布规则到 Nacos：

```bash
./scripts/publish-sentinel-rules-to-nacos.sh
```

启动独立 token server：

```bash
SENTINEL_CLUSTER_NAMESPACE=sentinel-bff-service \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
mvn -pl sentinel-token-server spring-boot:run
```

启动第一个 BFF client：

```bash
SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
mvn -pl sentinel-bff-service spring-boot:run \
  -Dspring-boot.run.arguments="--server.port=8086"
```

启动第二个 BFF client：

```bash
SENTINEL_CLUSTER_MODE=client \
SENTINEL_CLUSTER_SERVER_HOST=127.0.0.1 \
SENTINEL_CLUSTER_SERVER_PORT=18730 \
mvn -pl sentinel-bff-service spring-boot:run \
  -Dspring-boot.run.arguments="--server.port=8186"
```

如果 token server 和 BFF 不在同一台机器，把 `SENTINEL_CLUSTER_SERVER_HOST` 改成 token server 的内网 IP。

## 关键点

`sentinel-token-server` 只做集群令牌服务，不处理业务请求。

`sentinel-bff-service` 不再支持嵌入式 token server；它只通过：

```text
SENTINEL_CLUSTER_MODE=client
```

连接独立 token server。

# Sentinel Nacos Rules and FallbackFactory

这个实验仍然使用原来的入口：

```text
http://127.0.0.1:8080/api/lab-chain/entry
```

链路是：

```text
Gateway -> sentinel-bff-service -> sentinel-a-service -> sentinel-b-service
```

## Nacos 规则文件

启动 `sentinel-chain` 时，脚本会把下面三份配置发布到 Nacos 的 `SENTINEL_GROUP`：

```text
sentinel-bff-service-flow-rules.json
sentinel-bff-service-degrade-rules.json
sentinel-a-service-degrade-rules.json
```

对应本地文件在：

```text
nacos-config/sentinel/
```

服务通过 `spring.cloud.sentinel.datasource.*.nacos` 动态订阅这些规则。你在 Nacos 控制台修改 JSON 并发布后，服务会动态感知，不需要重启服务。

## 规则设计

### BFF 入口集群流控

DataId：

```text
sentinel-bff-service-flow-rules.json
```

资源：

```text
chainBffEntry
```

规则含义：

```text
两个 BFF 实例加起来，chainBffEntry 总 QPS > 3 时快速失败
```

演示命令：

```bash
./scripts/demo-sentinel-cluster-flow.sh
```

被限流时会看到类似：

```text
{"success":false,"message":"Sentinel blocked chainBffEntry: FlowException","data":null}
```

这个规则开启了：

```text
clusterMode=true
```

所以需要先启动独立 `sentinel-token-server`。`./scripts/lab-up sentinel-chain` 会自动启动 token server 和两个 BFF client。

如果你想恢复成本机 Web 入口资源限流，可以把规则改成：

```json
[
  {
    "resource": "/lab-chain/entry",
    "limitApp": "default",
    "grade": 0,
    "count": 150,
    "strategy": 0,
    "controlBehavior": 0,
    "clusterMode": false
  }
]
```

`grade: 1` 是 QPS，`grade: 0` 是并发线程数。Web 入口资源 `/lab-chain/entry` 被限流时走 `SentinelWebBlockHandler`，方法资源 `chainBffEntry` 被限流时走 `BffController.entryBlocked`。

### BFF 慢调用熔断

DataId：

```text
sentinel-bff-service-degrade-rules.json
```

资源：

```text
chainBffEntry
```

规则含义：

```text
10 秒统计窗口内至少 5 个请求，其中超过 60% 的请求耗时大于 500ms，就熔断 10 秒
```

演示命令：

```bash
for i in {1..6}; do curl -s 'http://127.0.0.1:8080/api/lab-chain/entry?slowMs=800'; echo; done
curl -s http://127.0.0.1:8080/api/lab-chain/entry
```

后面的正常请求也可能被短暂熔断，返回：

```text
Sentinel blocked chainBffEntry
```

### Sentinel Web 自定义兜底

`/lab-chain/entry` 是 Spring MVC Web 入口资源，它由 Sentinel Web 拦截器在进入 Controller 方法前检查。

这个资源被限流时，请求还没有进入 `BffController.entry(...)` 方法，所以不会调用 `@SentinelResource(value = "chainBffEntry", blockHandler = "entryBlocked")` 里的 `entryBlocked`。

Web 入口资源要用全局 `BlockExceptionHandler` 自定义返回：

```java
@Component
public class SentinelWebBlockHandler implements BlockExceptionHandler {

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, BlockException ex) throws Exception {
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(), ApiResponse.fail(messageOf(ex)));
    }
}
```

本项目的实现位置：

```text
sentinel-bff-service/src/main/java/com/example/hotel/sentinel/bff/config/SentinelWebBlockHandler.java
```

效果：

```text
Web 资源 /lab-chain/entry 被限流 -> SentinelWebBlockHandler
方法资源 chainBffEntry 被限流或熔断 -> BffController.entryBlocked
Feign 资源 GET:http://sentinel-b-service/b/work 被熔断 -> Feign fallbackFactory
```

### A 服务 Feign 异常比例熔断

DataId：

```text
sentinel-a-service-degrade-rules.json
```

资源：

```text
GET:http://sentinel-b-service/b/work
```

规则含义：

```text
10 秒统计窗口内至少 5 个请求，异常比例超过 50%，就熔断 10 秒
```

演示命令：

```bash
for i in {1..6}; do curl -s 'http://127.0.0.1:8080/api/lab-chain/entry?fail=true'; echo; done
curl -s http://127.0.0.1:8080/api/lab-chain/entry
```

前几次 `fail=true` 会让 B 服务返回 500，A 服务调用 B 服务的 Feign client 会进入 fallbackFactory。熔断打开后，即使最后一次不带 `fail=true`，A 服务也会直接走 fallbackFactory，不再真的调用 B 服务。

## fallbackFactory 怎么用

普通 fallback 写法是：

```java
@FeignClient(name = "sentinel-b-service", fallback = BServiceClientFallback.class)
```

这种写法能返回兜底结果，但拿不到具体原因。

fallbackFactory 写法是：

```java
@FeignClient(name = "sentinel-b-service", fallbackFactory = BServiceClientFallbackFactory.class)
```

工厂类：

```java
@Component
public class BServiceClientFallbackFactory implements FallbackFactory<BServiceClient> {

    @Override
    public BServiceClient create(Throwable cause) {
        return (slowMs, fail) -> ApiResponse.fail("FallbackFactory from sentinel-a-service to sentinel-b-service: "
                + cause.getClass().getSimpleName() + " - " + cause.getMessage());
    }
}
```

关键点：

```text
create(Throwable cause) 会在 Feign 调用失败或被 Sentinel 熔断时执行
cause 能告诉你失败原因
create 方法返回的是一个 Feign 接口的兜底实现
返回对象的方法签名必须和 Feign 接口一致
fallbackFactory 类要交给 Spring 管理，通常加 @Component
feign.sentinel.enabled=true 必须开启
```

常见 cause：

```text
FeignException$InternalServerError：下游返回 500
DegradeException：Sentinel 熔断打开，直接拦截
FlowException：Sentinel 流控拦截
RetryableException：连接失败、超时等网络问题
```

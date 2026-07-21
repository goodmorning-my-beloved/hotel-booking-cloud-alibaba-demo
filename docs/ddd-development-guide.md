# DDD 分层开发指引

这份手册用于新开发在工程里新增接口、用例、领域对象和基础设施代码时快速定位目录。当前项目是 Spring Cloud Alibaba 演示工程，不追求复杂 DDD 框架，但包结构按领域驱动设计的责任边界组织。

## 一、标准目录

每个业务服务以自己的 bounded context 作为根包，例如：

```text
order-service/src/main/java/com/example/hotel/order
message-service/src/main/java/com/example/hotel/message
user-service/src/main/java/com/example/hotel/user
hotel-service/src/main/java/com/example/hotel/hotel
payment-service/src/main/java/com/example/hotel/payment
```

根包下面按四层放代码：

```text
interfaces/rest
application/service
domain/model
domain/service
domain/repository
infrastructure/client
infrastructure/config
infrastructure/persistence
infrastructure/messaging
```

当前已落地的示例：

```text
order/interfaces/rest/OrderController.java
order/application/service/OrderService.java
order/domain/model/BookingStayPolicy.java
order/infrastructure/client/UserClient.java

message/interfaces/rest/MessageController.java
message/interfaces/rest/dto/RabbitMqDemoStatus.java
message/application/service/RabbitMqDemoService.java
message/domain/model/RabbitMqBookingMessage.java
message/infrastructure/config/RabbitMqDemoConfig.java
message/infrastructure/persistence/RabbitMqOutboxRepository.java
```

## 二、每层职责

`interfaces`

放对外入口，只处理协议适配，例如 HTTP Controller、请求参数、返回 DTO。不要在这里写业务规则、事务编排、远程调用编排。

`application`

放用例服务，负责串起一个业务动作。例如创建订单时：校验领域规则、调用用户/酒店/支付上下文、发布事件、处理事务。Application Service 可以依赖 domain，也可以依赖 infrastructure 的实现类或端口。

`domain`

放领域模型、值对象、领域规则、领域服务和仓储端口。这里应该表达业务概念，尽量不要依赖 Spring MVC、Feign、RabbitTemplate、JdbcTemplate 等技术细节。

`infrastructure`

放技术实现，例如 Feign Client、RabbitMQ 拓扑配置、RabbitMQ 监听/发送适配、JdbcTemplate Repository、外部系统 SDK 配置。这里可以依赖框架。

## 三、新增 HTTP 接口怎么写

假设要在 `order-service` 新增“取消订单”接口：

1. 在 `order/interfaces/rest` 新建或修改 Controller。

```text
order-service/src/main/java/com/example/hotel/order/interfaces/rest/OrderController.java
```

Controller 只做参数接收和响应包装：

```java
@PostMapping("/{orderId}/cancel")
public ApiResponse<BookingResponse> cancel(@PathVariable String orderId) {
    return ApiResponse.ok(orderService.cancel(orderId));
}
```

2. 在 `order/application/service` 增加用例方法。

```text
order-service/src/main/java/com/example/hotel/order/application/service/OrderService.java
```

Application Service 负责组织流程：

```java
public BookingResponse cancel(String orderId) {
    // 读取订单、校验状态、调用酒店释放房间、更新订单、发布取消事件。
}
```

3. 如果有业务规则，放到 `order/domain`。

例如“已支付订单取消必须走退款流程”“入住当天不可取消”，不要写在 Controller：

```text
order/domain/model/BookingCancelPolicy.java
order/domain/service/BookingDomainService.java
```

4. 如果要调外部服务，放到 `order/infrastructure/client`。

```text
order/infrastructure/client/PaymentClient.java
order/infrastructure/client/HotelClient.java
```

5. 如果要落库，优先定义仓储职责，再放实现。

简单 demo 可以直接放实现：

```text
order/infrastructure/persistence/JdbcOrderRepository.java
```

复杂业务建议先定义端口：

```text
order/domain/repository/OrderRepository.java
order/infrastructure/persistence/JdbcOrderRepository.java
```

## 四、新增 MQ 能力怎么写

以 `message-service` 为例：

1. HTTP 触发入口放：

```text
message/interfaces/rest/MessageController.java
```

2. 发布、重试、补偿等用例流程放：

```text
message/application/service/RabbitMqDemoService.java
```

3. 消息体、投递记录、事故任务等业务对象放：

```text
message/domain/model
```

4. RabbitMQ exchange、queue、binding、message converter 放：

```text
message/infrastructure/config/RabbitMqDemoConfig.java
```

5. 本地消息表、DLQ 事故表等数据库实现放：

```text
message/infrastructure/persistence
```

生产项目里可以进一步把 `@RabbitListener` 单独移到 `infrastructure/messaging`，再调用 `application/service`。当前 demo 为了便于课堂演示，把 listener 和用例服务放在同一个应用服务类里。

## 五、命名建议

Controller：

```text
XxxController
```

应用服务：

```text
XxxService
XxxApplicationService
```

领域规则：

```text
XxxPolicy
```

领域服务：

```text
XxxDomainService
```

仓储端口：

```text
XxxRepository
```

基础设施实现：

```text
JdbcXxxRepository
RabbitXxxPublisher
XxxClient
```

## 六、开发检查清单

新增接口时按这个顺序检查：

1. URL 和参数是否只出现在 `interfaces/rest`。
2. 业务流程是否在 `application/service`。
3. 业务规则是否抽到了 `domain`。
4. Feign、MQ、DB、配置是否放在 `infrastructure`。
5. 领域层是否没有依赖 Controller、Feign、RabbitTemplate、JdbcTemplate。
6. README 或 lab 脚本里的演示命令是否同步更新。
7. 提交前执行：

```bash
mvn -q -DskipTests package
bash -n scripts/lab-up
bash -n scripts/lab-down
git diff --check
```

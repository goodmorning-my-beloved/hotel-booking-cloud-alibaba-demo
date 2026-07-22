# DDD 分层开发指引

这份手册既是当前工程的开发约束，也是一个可以运行的轻量 DDD 示例。DDD 的目标不是凑齐四个目录，而是让业务语言、业务规则和技术实现拥有清晰边界。

当前酒店主链路划分为四个业务限界上下文：

```text
user-service       用户上下文
hotel-service      酒店库存上下文
order-service      预订订单上下文
payment-service    支付上下文
```

`message-service` 是学习可靠消息、Outbox 和 DLQ 的技术支撑上下文，不应把 `RabbitTemplate`、`Channel` 等技术概念扩散到酒店业务领域。

## 一、先理解依赖方向

本工程采用端口与适配器风格的轻量 DDD：

```text
HTTP / MQ 入站适配器
        |
        v
application/port/in  <- application/service -> application/port/out
                                |
                                v
                         domain model
                                ^
                                |
                  infrastructure 出站适配器
```

依赖必须由外向内：

1. `domain` 不依赖 `application`、`interfaces`、`infrastructure`、Spring 或共享协议 DTO。
2. `application` 可以依赖 `domain` 和自己定义的端口，不能直接依赖 Feign Client、RabbitTemplate、Controller DTO 或 JDBC 实现。
3. `interfaces` 和 `infrastructure` 是适配器，可以依赖应用端口和领域对象。
4. 跨上下文的 HTTP/MQ DTO 只在适配器边界转换，不作为本上下文的领域模型。

`order-service` 中的架构测试会自动保护前两条规则：

```text
order-service/src/test/java/com/example/hotel/order/architecture/LayerDependencyTest.java
```

## 二、推荐目录

```text
interfaces/rest
application/command
application/result
application/port/in
application/port/out
application/service
domain/model
domain/event
domain/policy
domain/repository
infrastructure/client
infrastructure/persistence
infrastructure/messaging
infrastructure/config
```

目录职责如下。

### interfaces

入站协议适配器。负责：

- HTTP 路由和参数接收；
- 将 HTTP DTO 转换成 Command；
- 调用入站用例端口；
- 将 Application Result 转换成 HTTP Response；
- 将 Sentinel 限流或异常转换成协议失败响应。

Controller 不保存业务数据，不计算价格，不修改库存，也不执行支付风控。

### application

应用层表达系统提供的用例。

```java
public interface BookRoomUseCase {
    BookingView bookRoom(BookRoomCommand command);
}
```

应用服务负责组织一个完整动作，例如：

1. 创建入住日期值对象；
2. 通过端口查询用户和房型；
3. 创建订单聚合；
4. 预留库存并支付；
5. 保存聚合并发布事件；
6. 失败时执行退款和库存释放补偿。

应用服务中的公开方法应当是用例或查询：

```text
bookRoom(command)       命令用例
getOrder(orderId)       单订单查询用例
listOrders()            订单列表查询用例
```

私有校验或映射方法不是用例，这没有问题。Sentinel `blockHandler`、RabbitMQ ACK、定时扫描等技术回调不是用例，不能放进应用服务充当公开业务 API。

### domain

领域层表达业务语言和永远成立的业务约束。

当前示例包括：

```text
order/domain/model/BookingOrder.java       订单聚合根
order/domain/model/StayPeriod.java         入住日期值对象
hotel/domain/model/Room.java               房型库存聚合根
payment/domain/model/Payment.java          支付聚合根
payment/domain/policy/PaymentRiskPolicy.java
```

领域对象应该保证“非法状态无法轻易产生”：

- `StayPeriod` 创建时保证离店日在入住日之后，并限制最多入住 30 晚；
- `BookingOrder` 只能从待支付状态确认支付一次；
- `Room` 用订单号保证预留幂等，未知订单的释放不会凭空增加库存；
- `Payment` 明确表达 `PAID -> REFUNDED` 状态迁移。

字段是否缺失通常属于接口或 Command 校验；“最多住 30 晚”“库存不能为负”“支付超过风控上限”等才是领域规则。

### infrastructure

基础设施层实现应用层定义的出站端口：

```text
FeignUserProfileGateway       实现 UserProfileGateway
FeignRoomInventoryGateway     实现 RoomInventoryGateway
FeignPaymentGateway           实现 PaymentGateway
InMemoryOrderRepository       实现 OrderRepository
StreamBookingEventPublisher   实现 BookingEventPublisher
```

Feign 的 `ApiResponse` 成败判断、Spring Cloud Stream binding 名、UUID 生成和内存 Map 都属于适配器细节，不进入领域模型。

## 三、Order 用例调用链

```text
OrderController
  -> BookRoomUseCase
  -> OrderApplicationService
       -> UserProfileGateway
       -> RoomInventoryGateway
       -> BookingOrder
       -> PaymentGateway
       -> OrderRepository
       -> BookingEventPublisher
```

这里有三个容易混淆的概念：

1. `BookRoomCommand` 是应用输入，不是领域实体。
2. `BookingOrder` 是带有状态与行为的聚合，不是 HTTP Response。
3. `BookingResponse` 是对外协议 DTO，只在 Controller 中映射。

## 四、跨限界上下文

订单上下文不能直接操作用户、酒店或支付上下文的聚合，只能通过本上下文定义的端口表达自己的需求：

```text
UserProfileGateway
RoomInventoryGateway
PaymentGateway
```

Feign Adapter 负责把远端 `UserDto`、`RoomDto`、`PaymentResponse` 翻译成本地需要的 `UserProfile`、`RoomOffer`、`PaymentReceipt`。这层翻译就是简化的防腐层。

`hotel-common` 目前只作为演示项目的发布协议模块。不要把其中的 DTO 引入 `domain`，也不要在共享模块中增加订单、库存或支付行为；否则多个上下文会重新耦合成一个共享领域模型。

## 五、事务与补偿

事务边界位于应用用例。当前订单用例保留 Seata 的 `@GlobalTransactional` 作为组件学习入口，同时对内存实现无法参与事务的步骤做显式补偿：

```text
支付失败       -> 释放房间
事件发布失败   -> 删除内存订单、退款、释放房间
```

注意：这只是教学实现，不等于生产级分布式一致性。RabbitMQ 和 Kafka 双写不可能靠一个本地事务变成原子操作。生产系统应使用业务库事务 + Outbox，分别记录每个发布目标的状态，并让消费者保持幂等。

补偿本身也可能失败。生产环境还需要补偿任务表、重试、告警和人工处理，而不是只依赖同步 `catch`。

## 六、MQ 适配器

`message-service` 将不同责任拆开：

```text
application/service/RabbitMqDemoApplicationService
    发布、查询和 DLQ 补偿用例

infrastructure/messaging/RabbitMqPublisherAdapter
    RabbitTemplate、publisher confirm、mandatory return

infrastructure/messaging/RabbitMqConsumerAdapter
    @RabbitListener、反序列化、ACK/NACK、DLQ 转存

infrastructure/messaging/RabbitMqOutboxRetryScheduler
    @Scheduled 定时重试入口
```

应用服务依赖 `RabbitMqMessagePublisher`、`RabbitMqOutboxStore`、`RabbitMqDlqIncidentStore` 等端口，不直接依赖 Spring AMQP。

## 七、新增用例的步骤

以“取消订单”为例：

1. 明确业务语言和不变量：什么状态可以取消，入住当天能否取消。
2. 在 `application/command` 定义 `CancelOrderCommand`。
3. 在 `application/port/in` 定义 `CancelOrderUseCase`。
4. 在聚合上增加 `order.cancel(...)`，由聚合保护状态转换。
5. 应用服务加载聚合、调用行为、保存聚合、退款、释放库存并发布事件。
6. 外部能力先定义出站端口，再在 `infrastructure` 实现。
7. Controller 只完成请求、Command、Result 三者之间的转换。
8. 先写领域规则测试，再写应用编排与补偿测试。

## 八、测试策略

测试按从内到外的顺序编写：

1. 值对象和聚合单元测试：不启动 Spring，快速验证不变量。
2. 应用服务测试：使用 Mock/Fake 端口验证调用顺序、保存和补偿。
3. 架构测试：防止 domain/application 反向依赖适配器。
4. Adapter 测试：验证 Feign、JDBC、MQ 和 JSON 映射。
5. 端到端测试：验证 HTTP 契约以及真实中间件协作。

本工程提交前至少执行：

```bash
mvn -q test
mvn -q -DskipTests package
bash -n scripts/lab-up
bash -n scripts/lab-down
bash -n scripts/smoke-test.sh
git diff --check
```

## 九、代码评审清单

1. 公开应用方法是否能用一个业务动作或查询来命名？
2. Controller 是否只做协议适配？
3. 领域不变量是否位于值对象、聚合或 Policy？
4. Application 是否只依赖 domain 和端口？
5. Domain 是否完全不知道 Spring、Feign、MQ、JDBC 和共享协议 DTO？
6. Map、数据库和缓存是否隐藏在 Repository 实现后？
7. 跨上下文 DTO 是否在 Adapter 中被翻译？
8. 技术回调是否位于 interfaces/infrastructure？
9. 事务失败、远程调用失败、事件失败是否有明确语义和补偿测试？
10. 是否存在架构测试防止后续代码破坏依赖方向？

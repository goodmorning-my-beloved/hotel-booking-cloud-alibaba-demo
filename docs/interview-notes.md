# 面试追问笔记

## Nacos

本项目用了 Nacos 的两个能力：

- 注册中心：所有服务启动后注册到 Nacos，Gateway 和 Feign 通过服务名调用。
- 配置中心：每个服务都有 `spring.config.import=optional:nacos:${spring.application.name}.yaml`。

常见追问：

- Nacos 和 Eureka 的区别？
- Nacos AP/CP 怎么选？
- 配置更新怎么动态刷新？
- 注册中心挂了，已发现服务还能不能调用？

## Gateway

Gateway 是统一入口，负责路由、鉴权、限流、跨域、灰度等横切逻辑。本项目用 `lb://service-name` 走服务发现。

常见追问：

- Gateway 和 Nginx 的边界是什么？
- Gateway 的过滤器顺序怎么控制？
- 网关限流和服务内限流有什么区别？

## OpenFeign

`order-service` 通过 Feign 调用：

- `user-service`
- `hotel-service`
- `payment-service`

Feign 的核心价值是把 HTTP 调用写成 Java 接口，并结合服务发现、负载均衡和降级。

常见追问：

- Feign 超时怎么配置？
- Feign 如何做降级？
- Feign 和 RestTemplate/WebClient 的区别？

## Sentinel

本项目在这些资源上加了 `@SentinelResource`：

- `bookRoom`
- `reserveRoom`
- `payOrder`

常见追问：

- Sentinel 和 Hystrix 的区别？
- QPS 限流、线程数限流、熔断降级分别解决什么问题？
- blockHandler 和 fallback 的区别？

## Seata

`OrderService#book` 使用 `@GlobalTransactional` 作为全局事务入口。

为了让 demo 更容易运行，当前业务数据使用内存存储。真实生产里要把库存、订单、支付落库，并按 Seata AT/TCC/Saga 的模式接入资源管理器。

常见追问：

- AT、TCC、Saga、XA 分别适合什么场景？
- Seata TC/TM/RM 分别是什么？
- 为什么 AT 模式需要 `undo_log`？
- 分布式事务和最终一致性如何取舍？

## RabbitMQ

订单创建后，`order-service` 通过 `StreamBridge` 把 `BookingCreatedEvent` 发送到 RabbitMQ 的 `hotel.booking.created` exchange，`message-service` 用 Spring Cloud Stream Consumer 消费。

常见追问：

- exchange、queue、binding、routing key 分别是什么？
- direct、topic、fanout exchange 的区别？
- 消息重复消费怎么处理？
- 消息确认、死信队列、重试队列怎么设计？

## Kafka

同一笔订单创建后也会发送到 Kafka topic `hotel-booking-created`，`message-service` 使用消费者组 `message-service` 消费。

常见追问：

- topic、partition、consumer group、offset 分别是什么？
- Kafka 为什么吞吐高？
- 如何保证分区内有序？
- 消费位移提交失败或重复消费怎么处理？

## 推荐回答思路

面试时不要只背组件名，要把组件放进业务链路里讲：

```text
用户请求先进 Gateway，Gateway 通过 Nacos 找到 order-service。
order-service 作为聚合服务，用 Feign 调用户、库存和支付。
热点接口由 Sentinel 保护，避免突发流量打垮服务。
预订主流程用 Seata 标出全局事务边界。
订单成功后同时发 RabbitMQ 和 Kafka 事件，方便学习两类消息中间件的控制台、消息流转和消费者组。
```

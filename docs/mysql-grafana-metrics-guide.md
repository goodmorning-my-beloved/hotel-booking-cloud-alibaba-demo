# MySQL and Connection Pool Metrics Guide

## Start

```bash
./scripts/lab-up jvm-gc
```

Open:

```text
Grafana: http://127.0.0.1:3000  admin/admin
Nacos:   http://127.0.0.1:8848/nacos
MySQL:   127.0.0.1:3306  hotel_demo/hotel_demo_pass
```

Dashboard:

```text
Hotel Demo / Hotel Demo MySQL and Connection Pool Metrics
```

## Tables

The demo uses one MySQL database:

```text
hotel_demo
```

Business tables:

```text
hotel_user
hotel_room
hotel_room_reservation
booking_order
payment
mq_outbox_message
mq_dlq_incident
kafka_demo_processed_message
kafka_demo_order_projection
kafka_demo_dlt_incident
```

Ownership:

```text
user-service     -> hotel_user
hotel-service    -> hotel_room, hotel_room_reservation
order-service    -> booking_order
payment-service  -> payment
message-service  -> mq_*, kafka_demo_*
```

## Nacos

The MySQL configuration is published from:

```text
nacos-config/apps/user-service.yaml
nacos-config/apps/hotel-service.yaml
nacos-config/apps/order-service.yaml
nacos-config/apps/payment-service.yaml
nacos-config/apps/message-service.yaml
```

Important fields:

```yaml
spring:
  datasource:
    url: jdbc:mysql://${MYSQL_HOST:mysql}:${MYSQL_PORT:3306}/${MYSQL_DATABASE:hotel_demo}
    username: ${MYSQL_USERNAME:hotel_demo}
    password: ${MYSQL_PASSWORD:hotel_demo_pass}
    hikari:
      maximum-pool-size: ${MYSQL_MAX_POOL_SIZE:6}
      minimum-idle: ${MYSQL_MIN_IDLE:1}
  sql:
    init:
      mode: always
```

`message-service` uses `schema-mysql.sql` because its old H2 schema is not valid MySQL DDL.

## Key Metrics

Connection pressure:

```promql
hikaricp_connections_active
hikaricp_connections_idle
hikaricp_connections_max
hikaricp_connections_pending
```

Pool usage:

```promql
100 * hikaricp_connections_active / hikaricp_connections_max
```

Connection timeout:

```promql
increase(hikaricp_connections_timeout_total[5m])
```

Average time to get a connection:

```promql
sum by (application) (rate(hikaricp_connections_acquire_seconds_sum[1m]))
/
sum by (application) (rate(hikaricp_connections_acquire_seconds_count[1m]))
```

## How To Read It

If `active` is close to `max` and `pending` is greater than 0, requests are waiting for a database connection. First check whether SQL is slow or blocked before increasing the pool.

If `timeout_total` increases, the application failed to get a connection within `connection-timeout`. Check MySQL health, slow SQL, locks, and pool size.

If `idle` is always high and `active` is low, the pool is larger than the current workload needs.

If `acquire time` rises while MySQL CPU or IO also rises, the database is probably the bottleneck. If MySQL is idle but `pending` rises, check leaked connections or long transactions in the application.

## Useful Commands

```bash
docker exec -it hotel-demo-mysql mysql -uhotel_demo -photel_demo_pass hotel_demo
```

```sql
SHOW TABLES;
SELECT COUNT(*) FROM mq_outbox_message;
SELECT COUNT(*) FROM kafka_demo_processed_message;
SELECT * FROM booking_order ORDER BY created_at DESC LIMIT 5;
```

```bash
curl -s 'http://127.0.0.1:9091/api/v1/query?query=hikaricp_connections_active'
```

CREATE TABLE IF NOT EXISTS mq_outbox_message (
    outbox_id VARCHAR(64) PRIMARY KEY,
    message_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    exchange_name VARCHAR(128) NOT NULL,
    routing_key VARCHAR(128) NOT NULL,
    payload_json CLOB NOT NULL,
    demo_note VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INT NOT NULL,
    max_attempts INT NOT NULL,
    next_retry_at TIMESTAMP NULL,
    last_error VARCHAR(1024) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mq_outbox_retry
    ON mq_outbox_message (status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_mq_outbox_updated
    ON mq_outbox_message (updated_at);

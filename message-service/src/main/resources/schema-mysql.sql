CREATE TABLE IF NOT EXISTS mq_outbox_message (
    outbox_id VARCHAR(64) PRIMARY KEY,
    message_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    exchange_name VARCHAR(128) NOT NULL,
    routing_key VARCHAR(128) NOT NULL,
    payload_json LONGTEXT NOT NULL,
    demo_note VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INT NOT NULL,
    max_attempts INT NOT NULL,
    next_retry_at TIMESTAMP NULL,
    last_error VARCHAR(1024) NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    INDEX idx_mq_outbox_retry (status, next_retry_at),
    INDEX idx_mq_outbox_updated (updated_at)
);

CREATE TABLE IF NOT EXISTS mq_dlq_incident (
    incident_id VARCHAR(64) PRIMARY KEY,
    source_key VARCHAR(256) NOT NULL,
    queue_name VARCHAR(128) NOT NULL,
    message_id VARCHAR(64) NULL,
    order_id VARCHAR(64) NULL,
    dead_letter_reason VARCHAR(128) NULL,
    payload_json LONGTEXT NOT NULL,
    headers_json LONGTEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    compensation_note VARCHAR(512) NULL,
    last_error VARCHAR(1024) NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    resolved_at TIMESTAMP(6) NULL,
    UNIQUE KEY uk_mq_dlq_incident_source (source_key),
    INDEX idx_mq_dlq_incident_status (status, updated_at)
);

CREATE TABLE IF NOT EXISTS kafka_demo_processed_message (
    message_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    business_key VARCHAR(128) NOT NULL,
    processed_at TIMESTAMP(6) NOT NULL,
    INDEX idx_kafka_demo_processed_at (processed_at)
);

CREATE TABLE IF NOT EXISTS kafka_demo_order_projection (
    order_id VARCHAR(64) PRIMARY KEY,
    message_id VARCHAR(64) NOT NULL,
    business_key VARCHAR(128) NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    UNIQUE KEY uk_kafka_demo_projection_message (message_id)
);

CREATE TABLE IF NOT EXISTS kafka_demo_dlt_incident (
    incident_id VARCHAR(64) PRIMARY KEY,
    source_key VARCHAR(256) NOT NULL,
    dlt_topic VARCHAR(256) NOT NULL,
    dlt_partition INT NOT NULL,
    dlt_offset BIGINT NOT NULL,
    original_topic VARCHAR(256) NULL,
    original_partition INT NULL,
    original_offset BIGINT NULL,
    message_id VARCHAR(64) NULL,
    business_key VARCHAR(128) NULL,
    poison_payload BOOLEAN NOT NULL,
    exception_type VARCHAR(512) NULL,
    exception_message VARCHAR(1024) NULL,
    payload_preview VARCHAR(1024) NOT NULL,
    payload_base64 LONGTEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    resolution_note VARCHAR(512) NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    resolved_at TIMESTAMP(6) NULL,
    UNIQUE KEY uk_kafka_demo_dlt_source (source_key),
    INDEX idx_kafka_demo_dlt_status (status, updated_at)
);

CREATE TABLE IF NOT EXISTS booking_order (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    room_id BIGINT NOT NULL,
    check_in DATE NOT NULL,
    check_out DATE NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    payment_id VARCHAR(64) NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_booking_order_user_created (user_id, created_at),
    INDEX idx_booking_order_room_created (room_id, created_at),
    INDEX idx_booking_order_status_created (status, created_at)
);

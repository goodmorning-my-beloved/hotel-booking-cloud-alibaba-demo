CREATE TABLE IF NOT EXISTS hotel_room (
    id BIGINT PRIMARY KEY,
    hotel_name VARCHAR(128) NOT NULL,
    room_type VARCHAR(128) NOT NULL,
    price_per_night DECIMAL(19, 2) NOT NULL,
    capacity INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hotel_room_reservation (
    room_id BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    check_in DATE NOT NULL,
    check_out DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (room_id, order_id),
    INDEX idx_hotel_room_reservation_order (order_id),
    CONSTRAINT fk_hotel_room_reservation_room
        FOREIGN KEY (room_id) REFERENCES hotel_room (id)
        ON DELETE CASCADE
);

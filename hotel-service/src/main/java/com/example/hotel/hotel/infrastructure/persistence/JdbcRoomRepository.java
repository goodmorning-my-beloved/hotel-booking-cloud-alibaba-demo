package com.example.hotel.hotel.infrastructure.persistence;

import com.example.hotel.hotel.domain.model.Room;
import com.example.hotel.hotel.domain.model.StayPeriod;
import com.example.hotel.hotel.domain.repository.RoomRepository;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class JdbcRoomRepository implements RoomRepository {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public JdbcRoomRepository(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public Optional<Room> findById(Long roomId) {
        List<Room> rows = jdbcTemplate.query("""
                        SELECT id, hotel_name, room_type, price_per_night, capacity
                        FROM hotel_room
                        WHERE id = ?
                        """,
                (rs, rowNum) -> {
                    Room room = new Room(
                            rs.getLong("id"),
                            rs.getString("hotel_name"),
                            rs.getString("room_type"),
                            rs.getBigDecimal("price_per_night"),
                            rs.getInt("capacity"));
                    loadReservations(room);
                    return room;
                },
                roomId);
        return rows.stream().findFirst();
    }

    @Override
    public List<Room> findAll() {
        return jdbcTemplate.query("""
                        SELECT id, hotel_name, room_type, price_per_night, capacity
                        FROM hotel_room
                        ORDER BY id
                        """,
                (rs, rowNum) -> {
                    Room room = new Room(
                            rs.getLong("id"),
                            rs.getString("hotel_name"),
                            rs.getString("room_type"),
                            rs.getBigDecimal("price_per_night"),
                            rs.getInt("capacity"));
                    loadReservations(room);
                    return room;
                });
    }

    @Override
    public void save(Room room) {
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update("""
                            INSERT INTO hotel_room (id, hotel_name, room_type, price_per_night, capacity)
                            VALUES (?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                                hotel_name = VALUES(hotel_name),
                                room_type = VALUES(room_type),
                                price_per_night = VALUES(price_per_night),
                                capacity = VALUES(capacity)
                            """,
                    room.id(),
                    room.hotelName(),
                    room.roomType(),
                    room.pricePerNight(),
                    room.capacity());

            jdbcTemplate.update("DELETE FROM hotel_room_reservation WHERE room_id = ?", room.id());
            List<Map.Entry<String, StayPeriod>> reservations = room.reservations().entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .toList();
            jdbcTemplate.batchUpdate("""
                            INSERT INTO hotel_room_reservation (room_id, order_id, check_in, check_out)
                            VALUES (?, ?, ?, ?)
                            """,
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Map.Entry<String, StayPeriod> row = reservations.get(i);
                            ps.setLong(1, room.id());
                            ps.setString(2, row.getKey());
                            ps.setObject(3, row.getValue().checkIn());
                            ps.setObject(4, row.getValue().checkOut());
                        }

                        @Override
                        public int getBatchSize() {
                            return reservations.size();
                        }
                    });
        });
    }

    private void loadReservations(Room room) {
        jdbcTemplate.query("""
                        SELECT order_id, check_in, check_out
                        FROM hotel_room_reservation
                        WHERE room_id = ?
                        ORDER BY order_id
                        """,
                (RowCallbackHandler) rs -> room.restoreReservation(
                        rs.getString("order_id"),
                        new StayPeriod(
                                rs.getDate("check_in").toLocalDate(),
                                rs.getDate("check_out").toLocalDate())),
                room.id());
    }
}

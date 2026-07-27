package com.example.hotel.user.infrastructure.persistence;

import com.example.hotel.user.domain.model.HotelUser;
import com.example.hotel.user.domain.model.MembershipLevel;
import com.example.hotel.user.domain.repository.UserRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class JdbcUserRepository implements UserRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcUserRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Optional<HotelUser> findById(Long userId) {
        List<HotelUser> rows = jdbcTemplate.query("""
                        SELECT id, name, membership_level
                        FROM hotel_user
                        WHERE id = ?
                        """,
                (rs, rowNum) -> new HotelUser(
                        rs.getLong("id"),
                        rs.getString("name"),
                        MembershipLevel.valueOf(rs.getString("membership_level"))),
                userId);
        return rows.stream().findFirst();
    }
}

package com.example.hotel.hotel.application.service;

import com.example.hotel.hotel.application.command.ReserveRoomCommand;
import com.example.hotel.hotel.application.port.in.RoomInventoryUseCase;
import com.example.hotel.hotel.application.result.RoomView;
import com.example.hotel.hotel.domain.model.Room;
import com.example.hotel.hotel.domain.model.StayPeriod;
import com.example.hotel.hotel.domain.repository.RoomRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RoomInventoryApplicationService implements RoomInventoryUseCase {

    private final RoomRepository roomRepository;

    public RoomInventoryApplicationService(RoomRepository roomRepository) {
        this.roomRepository = roomRepository;
    }

    @Override
    public List<RoomView> listRooms() {
        return roomRepository.findAll().stream().map(this::toView).toList();
    }

    @Override
    public Optional<RoomView> getRoom(Long roomId) {
        return roomRepository.findById(roomId).map(this::toView);
    }

    @Override
    public RoomView reserve(ReserveRoomCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("Reservation command is required.");
        }
        Room room = requiredRoom(command.roomId());
        room.reserve(command.orderId(), new StayPeriod(command.checkIn(), command.checkOut()));
        roomRepository.save(room);
        return toView(room);
    }

    @Override
    public RoomView release(Long roomId, String orderId) {
        Room room = requiredRoom(roomId);
        room.release(orderId);
        roomRepository.save(room);
        return toView(room);
    }

    private Room requiredRoom(Long roomId) {
        return roomRepository.findById(roomId)
                .orElseThrow(() -> new IllegalArgumentException("Room not found: " + roomId));
    }

    private RoomView toView(Room room) {
        return new RoomView(
                room.id(), room.hotelName(), room.roomType(), room.pricePerNight(), room.available());
    }
}

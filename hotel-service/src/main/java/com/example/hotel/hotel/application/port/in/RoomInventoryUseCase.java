package com.example.hotel.hotel.application.port.in;

import com.example.hotel.hotel.application.command.ReserveRoomCommand;
import com.example.hotel.hotel.application.result.RoomView;

import java.util.List;
import java.util.Optional;

public interface RoomInventoryUseCase {

    List<RoomView> listRooms();

    Optional<RoomView> getRoom(Long roomId);

    RoomView reserve(ReserveRoomCommand command);

    RoomView release(Long roomId, String orderId);
}

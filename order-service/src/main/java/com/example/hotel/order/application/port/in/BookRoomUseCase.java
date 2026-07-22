package com.example.hotel.order.application.port.in;

import com.example.hotel.order.application.command.BookRoomCommand;
import com.example.hotel.order.application.result.BookingView;

public interface BookRoomUseCase {

    BookingView bookRoom(BookRoomCommand command);
}

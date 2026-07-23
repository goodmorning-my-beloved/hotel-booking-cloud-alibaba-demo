package com.example.hotel.order.infrastructure.client;

import com.example.hotel.order.application.port.out.RoomInventoryGateway;
import com.example.hotel.order.domain.model.StayPeriod;
import com.example.hotel.order.infrastructure.client.dto.ReserveRoomApiRequest;
import com.example.hotel.order.infrastructure.client.dto.RoomApiResponse;
import org.springframework.stereotype.Component;

@Component
public class FeignRoomInventoryGateway implements RoomInventoryGateway {

    private final HotelClient hotelClient;

    public FeignRoomInventoryGateway(HotelClient hotelClient) {
        this.hotelClient = hotelClient;
    }

    @Override
    public RoomOffer getRequiredRoom(Long roomId) {
        RoomApiResponse room = RemoteResponse.requireData(hotelClient.findRoom(roomId), "Room check failed");
        return new RoomOffer(room.id(), room.price());
    }

    @Override
    public void reserve(Long roomId, String orderId, StayPeriod stayPeriod) {
        RemoteResponse.requireData(hotelClient.reserve(roomId,
                new ReserveRoomApiRequest(orderId, stayPeriod.checkIn(), stayPeriod.checkOut())),
                "Reserve room failed");
    }

    @Override
    public void release(Long roomId, String orderId) {
        RemoteResponse.requireData(hotelClient.release(roomId, orderId), "Release room failed");
    }
}

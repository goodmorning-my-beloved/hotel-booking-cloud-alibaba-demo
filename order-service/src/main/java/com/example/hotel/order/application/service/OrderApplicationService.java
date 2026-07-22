package com.example.hotel.order.application.service;

import com.example.hotel.order.application.command.BookRoomCommand;
import com.example.hotel.order.application.port.in.BookRoomUseCase;
import com.example.hotel.order.application.port.in.OrderQueryUseCase;
import com.example.hotel.order.application.port.out.BookingEventPublisher;
import com.example.hotel.order.application.port.out.OrderIdGenerator;
import com.example.hotel.order.application.port.out.PaymentGateway;
import com.example.hotel.order.application.port.out.RoomInventoryGateway;
import com.example.hotel.order.application.port.out.UserProfileGateway;
import com.example.hotel.order.application.result.BookingView;
import com.example.hotel.order.domain.event.BookingCreated;
import com.example.hotel.order.domain.model.BookingOrder;
import com.example.hotel.order.domain.model.StayPeriod;
import com.example.hotel.order.domain.repository.OrderRepository;
import io.seata.spring.annotation.GlobalTransactional;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * 订单应用服务：每个公开方法对应一个业务用例或查询用例。
 */
@Service
public class OrderApplicationService implements BookRoomUseCase, OrderQueryUseCase {

    private final UserProfileGateway userProfileGateway;
    private final RoomInventoryGateway roomInventoryGateway;
    private final PaymentGateway paymentGateway;
    private final BookingEventPublisher eventPublisher;
    private final OrderRepository orderRepository;
    private final OrderIdGenerator orderIdGenerator;

    public OrderApplicationService(UserProfileGateway userProfileGateway,
                                   RoomInventoryGateway roomInventoryGateway,
                                   PaymentGateway paymentGateway,
                                   BookingEventPublisher eventPublisher,
                                   OrderRepository orderRepository,
                                   OrderIdGenerator orderIdGenerator) {
        this.userProfileGateway = userProfileGateway;
        this.roomInventoryGateway = roomInventoryGateway;
        this.paymentGateway = paymentGateway;
        this.eventPublisher = eventPublisher;
        this.orderRepository = orderRepository;
        this.orderIdGenerator = orderIdGenerator;
    }

    @Override
    @GlobalTransactional(name = "hotel-booking-create", rollbackFor = Exception.class)
    public BookingView bookRoom(BookRoomCommand command) {
        requireCommand(command);
        StayPeriod stayPeriod = new StayPeriod(command.checkIn(), command.checkOut());
        UserProfileGateway.UserProfile user = userProfileGateway.getRequiredUser(command.userId());
        RoomInventoryGateway.RoomOffer room = roomInventoryGateway.getRequiredRoom(command.roomId());
        String orderId = orderIdGenerator.nextId();
        BigDecimal amount = room.price().multiply(BigDecimal.valueOf(stayPeriod.nights()));
        BookingOrder order = BookingOrder.createPending(
                orderId, user.id(), room.id(), stayPeriod, amount, Instant.now());

        boolean roomReserved = false;
        boolean paymentCompleted = false;
        try {
            roomInventoryGateway.reserve(room.id(), orderId, stayPeriod);
            roomReserved = true;

            PaymentGateway.PaymentReceipt payment = paymentGateway.pay(orderId, user.id(), amount);
            paymentCompleted = "PAID".equalsIgnoreCase(payment.status());
            if (!paymentCompleted) {
                throw new IllegalStateException("Payment did not reach PAID status: " + payment.status());
            }
            order.confirmPayment(payment.paymentId());
            orderRepository.save(order);

            eventPublisher.publish(new BookingCreated(
                    order.orderId(), order.userId(), order.roomId(), order.amount(), order.createdAt()));
            return toView(order);
        } catch (RuntimeException failure) {
            compensate(orderId, room.id(), paymentCompleted, roomReserved, failure);
            throw failure;
        }
    }

    @Override
    public Optional<BookingView> getOrder(String orderId) {
        return orderRepository.findById(orderId).map(this::toView);
    }

    @Override
    public List<BookingView> listOrders() {
        return orderRepository.findAll().stream().map(this::toView).toList();
    }

    private void compensate(String orderId,
                            Long roomId,
                            boolean paymentCompleted,
                            boolean roomReserved,
                            RuntimeException failure) {
        orderRepository.deleteById(orderId);
        if (paymentCompleted) {
            try {
                paymentGateway.refund(orderId);
            } catch (RuntimeException compensationFailure) {
                failure.addSuppressed(compensationFailure);
            }
        }
        if (roomReserved) {
            try {
                roomInventoryGateway.release(roomId, orderId);
            } catch (RuntimeException compensationFailure) {
                failure.addSuppressed(compensationFailure);
            }
        }
    }

    private BookingView toView(BookingOrder order) {
        return new BookingView(
                order.orderId(),
                order.userId(),
                order.roomId(),
                order.stayPeriod().checkIn(),
                order.stayPeriod().checkOut(),
                order.amount(),
                order.status().name());
    }

    private void requireCommand(BookRoomCommand command) {
        if (command == null || command.userId() == null || command.roomId() == null) {
            throw new IllegalArgumentException("userId and roomId are required.");
        }
    }
}

package com.example.hotel.order.application.service;

import com.example.hotel.order.application.command.BookRoomCommand;
import com.example.hotel.order.application.port.out.BookingEventPublisher;
import com.example.hotel.order.application.port.out.OrderIdGenerator;
import com.example.hotel.order.application.port.out.PaymentGateway;
import com.example.hotel.order.application.port.out.RoomInventoryGateway;
import com.example.hotel.order.application.port.out.UserProfileGateway;
import com.example.hotel.order.domain.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderApplicationServiceTest {

    @Mock
    private UserProfileGateway userGateway;
    @Mock
    private RoomInventoryGateway roomGateway;
    @Mock
    private PaymentGateway paymentGateway;
    @Mock
    private BookingEventPublisher eventPublisher;
    @Mock
    private OrderRepository orderRepository;
    @Mock
    private OrderIdGenerator orderIdGenerator;

    private OrderApplicationService service;

    @BeforeEach
    void setUp() {
        service = new OrderApplicationService(
                userGateway, roomGateway, paymentGateway, eventPublisher, orderRepository, orderIdGenerator);
        when(userGateway.getRequiredUser(1L))
                .thenReturn(new UserProfileGateway.UserProfile(1L, "Alice", "GOLD"));
        when(roomGateway.getRequiredRoom(101L))
                .thenReturn(new RoomInventoryGateway.RoomOffer(101L, new BigDecimal("688.00")));
        when(orderIdGenerator.nextId()).thenReturn("ORD-1");
    }

    @Test
    void booksRoomAsOneApplicationUseCase() {
        when(paymentGateway.pay("ORD-1", 1L, new BigDecimal("1376.00")))
                .thenReturn(new PaymentGateway.PaymentReceipt("PAY-1", "PAID"));

        var result = service.bookRoom(command());

        assertThat(result.orderId()).isEqualTo("ORD-1");
        assertThat(result.amount()).isEqualByComparingTo("1376.00");
        assertThat(result.status()).isEqualTo("PAID");
        verify(roomGateway).reserve(any(), any(), any());
        verify(orderRepository).save(any());
        verify(eventPublisher).publish(any());
    }

    @Test
    void compensatesPaymentAndInventoryWhenEventPublicationFails() {
        when(paymentGateway.pay("ORD-1", 1L, new BigDecimal("1376.00")))
                .thenReturn(new PaymentGateway.PaymentReceipt("PAY-1", "PAID"));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(eventPublisher).publish(any());

        assertThatThrownBy(() -> service.bookRoom(command()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broker unavailable");

        verify(paymentGateway).refund("ORD-1");
        verify(roomGateway).release(101L, "ORD-1");
        verify(orderRepository).deleteById("ORD-1");
    }

    @Test
    void doesNotRefundWhenPaymentNeverCompleted() {
        when(paymentGateway.pay(any(), any(), any()))
                .thenThrow(new IllegalStateException("payment rejected"));

        assertThatThrownBy(() -> service.bookRoom(command()))
                .isInstanceOf(IllegalStateException.class);

        verify(paymentGateway, never()).refund(any());
        verify(roomGateway).release(101L, "ORD-1");
    }

    private BookRoomCommand command() {
        return new BookRoomCommand(
                1L, 101L, LocalDate.of(2026, 8, 1), LocalDate.of(2026, 8, 3));
    }
}

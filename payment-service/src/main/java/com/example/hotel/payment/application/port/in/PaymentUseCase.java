package com.example.hotel.payment.application.port.in;

import com.example.hotel.payment.application.command.PayOrderCommand;
import com.example.hotel.payment.application.result.PaymentView;

public interface PaymentUseCase {

    PaymentView pay(PayOrderCommand command);

    PaymentView refund(String orderId);
}

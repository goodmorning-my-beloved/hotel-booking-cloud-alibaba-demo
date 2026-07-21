package com.example.hotel.order.domain.model;

import com.example.hotel.common.dto.BookingRequest;

import java.time.temporal.ChronoUnit;

/**
 * 订单领域里的入住规则。
 *
 * <p>Controller 只负责接收请求，Application Service 负责组织用例流程；
 * 像“必须有用户、房间、入住和离店日期”“最多住 30 晚”这类业务规则放在领域层，后续复用和测试更清晰。</p>
 */
public final class BookingStayPolicy {

    private BookingStayPolicy() {
    }

    public static void validate(BookingRequest request) {
        if (request.userId() == null || request.roomId() == null
                || request.checkIn() == null || request.checkOut() == null) {
            throw new IllegalArgumentException("userId, roomId, checkIn and checkOut are required.");
        }
        long nights = ChronoUnit.DAYS.between(request.checkIn(), request.checkOut());
        if (nights <= 0 || nights > 30) {
            throw new IllegalArgumentException("Stay length must be between 1 and 30 nights.");
        }
    }
}

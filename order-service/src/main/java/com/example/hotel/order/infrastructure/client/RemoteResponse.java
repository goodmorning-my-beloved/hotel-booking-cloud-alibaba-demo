package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;

final class RemoteResponse {

    private RemoteResponse() {
    }

    static <T> T requireData(ApiResponse<T> response, String operation) {
        if (response == null || !response.success() || response.data() == null) {
            String message = response == null ? "empty response" : response.message();
            throw new IllegalStateException(operation + ": " + message);
        }
        return response.data();
    }
}

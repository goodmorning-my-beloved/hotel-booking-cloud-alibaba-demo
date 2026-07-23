package com.example.hotel.message.application.result;

public record JvmGcDemoActionResult(
        String action,
        boolean accepted,
        long allocatedBytes,
        int allocatedObjectCount,
        boolean retained,
        String message,
        JvmGcDemoStatus status
) {
}

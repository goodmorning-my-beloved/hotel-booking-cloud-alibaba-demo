package com.example.hotel.message;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/messages")
public class MessageController {

    private final RabbitMqDemoService rabbitMqDemoService;

    public MessageController(RabbitMqDemoService rabbitMqDemoService) {
        this.rabbitMqDemoService = rabbitMqDemoService;
    }

    @PostMapping("/rabbitmq/demo/normal")
    public ApiResponse<RabbitMqDemoPublishResult> publishNormal(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return ApiResponse.ok(rabbitMqDemoService.publishNormal(messageId));
    }

    @PostMapping("/rabbitmq/demo/duplicate")
    public ApiResponse<List<RabbitMqDemoPublishResult>> publishDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return ApiResponse.ok(rabbitMqDemoService.publishDuplicate(messageId));
    }

    @PostMapping("/rabbitmq/demo/dead-letter")
    public ApiResponse<RabbitMqDemoPublishResult> publishDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return ApiResponse.ok(rabbitMqDemoService.publishDeadLetter(messageId));
    }

    @PostMapping("/rabbitmq/demo/unroutable")
    public ApiResponse<RabbitMqDemoPublishResult> publishUnroutable(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return ApiResponse.ok(rabbitMqDemoService.publishUnroutable(messageId));
    }

    @GetMapping("/rabbitmq/demo/status")
    public ApiResponse<RabbitMqDemoStatus> rabbitMqDemoStatus() {
        return ApiResponse.ok(rabbitMqDemoService.status());
    }
}

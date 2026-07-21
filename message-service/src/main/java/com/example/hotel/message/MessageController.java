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
        // 普通可靠投递演示：persistent message + publisher confirm + consumer manual ack。
        return ApiResponse.ok(rabbitMqDemoService.publishNormal(messageId));
    }

    @PostMapping("/rabbitmq/demo/duplicate")
    public ApiResponse<List<RabbitMqDemoPublishResult>> publishDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 幂等演示：连续发送两条相同 messageId 的消息，消费者只执行业务一次。
        return ApiResponse.ok(rabbitMqDemoService.publishDuplicate(messageId));
    }

    @PostMapping("/rabbitmq/demo/dead-letter")
    public ApiResponse<RabbitMqDemoPublishResult> publishDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 死信演示：消费者模拟失败并 nack(requeue=false)，消息进入 DLQ。
        return ApiResponse.ok(rabbitMqDemoService.publishDeadLetter(messageId));
    }

    @PostMapping("/rabbitmq/demo/dead-letter/resolve-next")
    public ApiResponse<RabbitMqDlqResolveResult> resolveNextDeadLetter(
            @RequestParam(value = "compensationNote", required = false) String compensationNote) {
        // DLQ 补偿演示：从死信队列取下一条消息，模拟人工排障/补偿完成后 basicAck 删除。
        return ApiResponse.ok(rabbitMqDemoService.resolveNextDeadLetter(compensationNote));
    }

    @PostMapping("/rabbitmq/demo/unroutable")
    public ApiResponse<RabbitMqDemoPublishResult> publishUnroutable(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 不可路由演示：routing key 没有绑定队列，mandatory return 会把问题反馈给生产者。
        return ApiResponse.ok(rabbitMqDemoService.publishUnroutable(messageId));
    }

    @GetMapping("/rabbitmq/demo/status")
    public ApiResponse<RabbitMqDemoStatus> rabbitMqDemoStatus() {
        // 状态接口用于把发布、确认、退回、消费、重复、死信计数串起来观察。
        return ApiResponse.ok(rabbitMqDemoService.status());
    }
}

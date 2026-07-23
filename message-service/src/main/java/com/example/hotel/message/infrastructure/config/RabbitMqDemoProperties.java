package com.example.hotel.message.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "hotel.rabbitmq.demo")
public class RabbitMqDemoProperties {

    private String bookingExchange = "hotel.booking.exchange";
    private String bookingRoutingKey = "hotel.booking.created";
    private String bookingQueue = "hotel.booking.created.queue";
    private String deadLetterExchange = "hotel.booking.dlx";
    private String deadLetterRoutingKey = "hotel.booking.created.dead";
    private String deadLetterQueue = "hotel.booking.created.dlq";
    private String topicExchange = "hotel.booking.topic.exchange";
    private String topicSingleWordPattern = "hotel.booking.*";
    private String topicMultiWordPattern = "hotel.booking.#";
    private String topicSingleWordQueue = "hotel.booking.topic.single-word.queue";
    private String topicMultiWordQueue = "hotel.booking.topic.multi-word.queue";
    private String fanoutExchange = "hotel.booking.fanout.exchange";
    private String fanoutSmsQueue = "hotel.booking.fanout.sms.queue";
    private String fanoutPointsQueue = "hotel.booking.fanout.points.queue";

    public String getBookingExchange() {
        return bookingExchange;
    }

    public void setBookingExchange(String bookingExchange) {
        this.bookingExchange = bookingExchange;
    }

    public String getBookingRoutingKey() {
        return bookingRoutingKey;
    }

    public void setBookingRoutingKey(String bookingRoutingKey) {
        this.bookingRoutingKey = bookingRoutingKey;
    }

    public String getBookingQueue() {
        return bookingQueue;
    }

    public void setBookingQueue(String bookingQueue) {
        this.bookingQueue = bookingQueue;
    }

    public String getDeadLetterExchange() {
        return deadLetterExchange;
    }

    public void setDeadLetterExchange(String deadLetterExchange) {
        this.deadLetterExchange = deadLetterExchange;
    }

    public String getDeadLetterRoutingKey() {
        return deadLetterRoutingKey;
    }

    public void setDeadLetterRoutingKey(String deadLetterRoutingKey) {
        this.deadLetterRoutingKey = deadLetterRoutingKey;
    }

    public String getDeadLetterQueue() {
        return deadLetterQueue;
    }

    public void setDeadLetterQueue(String deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
    }

    public String getTopicExchange() {
        return topicExchange;
    }

    public void setTopicExchange(String topicExchange) {
        this.topicExchange = topicExchange;
    }

    public String getTopicSingleWordPattern() {
        return topicSingleWordPattern;
    }

    public void setTopicSingleWordPattern(String topicSingleWordPattern) {
        this.topicSingleWordPattern = topicSingleWordPattern;
    }

    public String getTopicMultiWordPattern() {
        return topicMultiWordPattern;
    }

    public void setTopicMultiWordPattern(String topicMultiWordPattern) {
        this.topicMultiWordPattern = topicMultiWordPattern;
    }

    public String getTopicSingleWordQueue() {
        return topicSingleWordQueue;
    }

    public void setTopicSingleWordQueue(String topicSingleWordQueue) {
        this.topicSingleWordQueue = topicSingleWordQueue;
    }

    public String getTopicMultiWordQueue() {
        return topicMultiWordQueue;
    }

    public void setTopicMultiWordQueue(String topicMultiWordQueue) {
        this.topicMultiWordQueue = topicMultiWordQueue;
    }

    public String getFanoutExchange() {
        return fanoutExchange;
    }

    public void setFanoutExchange(String fanoutExchange) {
        this.fanoutExchange = fanoutExchange;
    }

    public String getFanoutSmsQueue() {
        return fanoutSmsQueue;
    }

    public void setFanoutSmsQueue(String fanoutSmsQueue) {
        this.fanoutSmsQueue = fanoutSmsQueue;
    }

    public String getFanoutPointsQueue() {
        return fanoutPointsQueue;
    }

    public void setFanoutPointsQueue(String fanoutPointsQueue) {
        this.fanoutPointsQueue = fanoutPointsQueue;
    }
}

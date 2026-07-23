package com.example.hotel.message.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "hotel.kafka.demo")
public class KafkaDemoProperties {

    private String topic = "hotel.kafka.demo.orders";
    private String deadLetterTopic = "hotel.kafka.demo.orders.DLT";
    private int partitions = 3;
    private int replicas = 1;
    private long retentionMs = 86400000L;
    private long deadLetterRetentionMs = 604800000L;
    private String acks = "all";
    private boolean idempotenceEnabled = true;
    private int retries = Integer.MAX_VALUE;
    private int lingerMs = 5;
    private int batchSize = 32768;
    private String compressionType = "lz4";
    private int deliveryTimeoutMs = 120000;
    private int maxInFlightRequestsPerConnection = 5;
    private String transactionIdPrefix = "hotel-kafka-demo-local-";
    private int maxPollRecords = 10;
    private String autoOffsetReset = "earliest";
    private int listenerConcurrency = 2;
    private long retryBackoffMs = 500L;
    private long retryMaxRetries = 2L;
    private ConsumerGroups consumerGroups = new ConsumerGroups();

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDeadLetterTopic() {
        return deadLetterTopic;
    }

    public void setDeadLetterTopic(String deadLetterTopic) {
        this.deadLetterTopic = deadLetterTopic;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public long getRetentionMs() {
        return retentionMs;
    }

    public void setRetentionMs(long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public long getDeadLetterRetentionMs() {
        return deadLetterRetentionMs;
    }

    public void setDeadLetterRetentionMs(long deadLetterRetentionMs) {
        this.deadLetterRetentionMs = deadLetterRetentionMs;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public boolean isIdempotenceEnabled() {
        return idempotenceEnabled;
    }

    public void setIdempotenceEnabled(boolean idempotenceEnabled) {
        this.idempotenceEnabled = idempotenceEnabled;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public void setDeliveryTimeoutMs(int deliveryTimeoutMs) {
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    public int getMaxInFlightRequestsPerConnection() {
        return maxInFlightRequestsPerConnection;
    }

    public void setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    public String getTransactionIdPrefix() {
        return transactionIdPrefix;
    }

    public void setTransactionIdPrefix(String transactionIdPrefix) {
        this.transactionIdPrefix = transactionIdPrefix;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public int getListenerConcurrency() {
        return listenerConcurrency;
    }

    public void setListenerConcurrency(int listenerConcurrency) {
        this.listenerConcurrency = listenerConcurrency;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public long getRetryMaxRetries() {
        return retryMaxRetries;
    }

    public void setRetryMaxRetries(long retryMaxRetries) {
        this.retryMaxRetries = retryMaxRetries;
    }

    public ConsumerGroups getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(ConsumerGroups consumerGroups) {
        this.consumerGroups = consumerGroups;
    }

    public static class ConsumerGroups {

        private String primary = "hotel-kafka-demo-primary";
        private String audit = "hotel-kafka-demo-audit";
        private String deadLetter = "hotel-kafka-demo-dlt";

        public String getPrimary() {
            return primary;
        }

        public void setPrimary(String primary) {
            this.primary = primary;
        }

        public String getAudit() {
            return audit;
        }

        public void setAudit(String audit) {
            this.audit = audit;
        }

        public String getDeadLetter() {
            return deadLetter;
        }

        public void setDeadLetter(String deadLetter) {
            this.deadLetter = deadLetter;
        }
    }
}

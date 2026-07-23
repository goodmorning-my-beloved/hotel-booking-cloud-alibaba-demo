package com.example.hotel.message.infrastructure.config;

import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@EnableKafka
@Configuration
@EnableConfigurationProperties(KafkaDemoProperties.class)
public class KafkaDemoConfig implements KafkaDemoTopology {

    @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}")
    private String bootstrapServers;

    private final KafkaDemoProperties properties;

    public KafkaDemoConfig(KafkaDemoProperties properties) {
        this.properties = properties;
    }

    @Bean
    NewTopic kafkaDemoTopic() {
        return TopicBuilder.name(properties.getTopic())
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .config("retention.ms", Long.toString(properties.getRetentionMs()))
                .build();
    }

    @Bean
    NewTopic kafkaDemoDeadLetterTopic() {
        return TopicBuilder.name(properties.getDeadLetterTopic())
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .config("retention.ms", Long.toString(properties.getDeadLetterRetentionMs()))
                .build();
    }

    @Bean
    ProducerFactory<String, Object> kafkaDemoProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, properties.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, properties.isIdempotenceEnabled());
        props.put(ProducerConfig.RETRIES_CONFIG, properties.getRetries());
        props.put(ProducerConfig.LINGER_MS_CONFIG, properties.getLingerMs());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getBatchSize());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getCompressionType());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, properties.getDeliveryTimeoutMs());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                properties.getMaxInFlightRequestsPerConnection());

        JsonSerializer<Object> jsonSerializer = new JsonSerializer<>();
        jsonSerializer.setAddTypeInfo(false);
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(KafkaDemoMessage.class, jsonSerializer);

        DefaultKafkaProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                new DelegatingByTypeSerializer(delegates));
        // 每个应用实例必须使用唯一前缀，Broker 才能 fencing 掉旧实例的 zombie producer。
        producerFactory.setTransactionIdPrefix(properties.getTransactionIdPrefix());
        return producerFactory;
    }

    @Bean
    KafkaTemplate<String, Object> kafkaDemoTemplate(
            @Qualifier("kafkaDemoProducerFactory") ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        // 普通演示仍允许非事务发送；事务接口显式使用 executeInTransaction。
        kafkaTemplate.setAllowNonTransactional(true);
        return kafkaTemplate;
    }

    @Bean
    DefaultKafkaConsumerFactory<String, KafkaDemoMessage> kafkaDemoConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getMaxPollRecords());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        JsonDeserializer<KafkaDemoMessage> jsonDeserializer =
                new JsonDeserializer<>(KafkaDemoMessage.class, false);
        jsonDeserializer.addTrustedPackages("com.example.hotel.message.domain.model");
        ErrorHandlingDeserializer<KafkaDemoMessage> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer);
    }

    @Bean
    DefaultKafkaConsumerFactory<String, byte[]> kafkaDemoDltConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getMaxPollRecords());
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ByteArrayDeserializer());
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, KafkaDemoMessage> kafkaDemoListenerContainerFactory(
            @Qualifier("kafkaDemoConsumerFactory") DefaultKafkaConsumerFactory<String, KafkaDemoMessage> consumerFactory,
            @Qualifier("kafkaDemoErrorHandler") DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaDemoMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(properties.getListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaDemoDltListenerContainerFactory(
            @Qualifier("kafkaDemoDltConsumerFactory")
            DefaultKafkaConsumerFactory<String, byte[]> consumerFactory,
            @Qualifier("kafkaDemoDltErrorHandler") DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(properties.getListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    DefaultErrorHandler kafkaDemoErrorHandler(
            @Qualifier("kafkaDemoTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(properties.getDeadLetterTopic(), record.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(properties.getRetryBackoffMs(), properties.getRetryMaxRetries()));
        // MANUAL_IMMEDIATE 下，消息成功写入 DLT 后提交原 topic offset，避免无限重复恢复。
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    DefaultErrorHandler kafkaDemoDltErrorHandler() {
        // DLT 已是最后防线；持久化工单失败时不能再投回同一个 DLT 形成循环。
        return new DefaultErrorHandler(
                new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }

    @Override
    public String demoTopic() {
        return properties.getTopic();
    }

    @Override
    public String deadLetterTopic() {
        return properties.getDeadLetterTopic();
    }

    @Override
    public String primaryConsumerGroup() {
        return properties.getConsumerGroups().getPrimary();
    }

    @Override
    public String auditConsumerGroup() {
        return properties.getConsumerGroups().getAudit();
    }

    @Override
    public String deadLetterConsumerGroup() {
        return properties.getConsumerGroups().getDeadLetter();
    }

    @Override
    public int partitionCount() {
        return properties.getPartitions();
    }

    @Override
    public String keyForPartition(int partition) {
        int target = Math.floorMod(partition, properties.getPartitions());
        for (int candidate = 0; candidate < 100_000; candidate++) {
            String key = "partition-" + target + "-key-" + candidate;
            int actual = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)))
                    % properties.getPartitions();
            if (actual == target) {
                return key;
            }
        }
        throw new IllegalStateException("Unable to find Kafka key for partition " + target);
    }

    @Override
    public List<String> demoTopics() {
        return List.of(properties.getTopic(), properties.getDeadLetterTopic());
    }

    @Override
    public List<String> demoConsumerGroups() {
        return List.of(
                properties.getConsumerGroups().getPrimary(),
                properties.getConsumerGroups().getAudit(),
                properties.getConsumerGroups().getDeadLetter());
    }
}

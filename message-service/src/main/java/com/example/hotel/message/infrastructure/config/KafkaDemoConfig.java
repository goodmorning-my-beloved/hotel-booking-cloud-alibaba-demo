package com.example.hotel.message.infrastructure.config;

import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
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
    ProducerFactory<String, KafkaDemoMessage> kafkaDemoProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, properties.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, properties.isIdempotenceEnabled());
        props.put(ProducerConfig.RETRIES_CONFIG, properties.getRetries());
        props.put(ProducerConfig.LINGER_MS_CONFIG, properties.getLingerMs());
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, KafkaDemoMessage> kafkaDemoTemplate(
            @Qualifier("kafkaDemoProducerFactory") ProducerFactory<String, KafkaDemoMessage> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    DefaultKafkaConsumerFactory<String, KafkaDemoMessage> kafkaDemoConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getMaxPollRecords());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, KafkaDemoMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.hotel.message.domain.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(props);
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
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    DefaultErrorHandler kafkaDemoErrorHandler(
            @Qualifier("kafkaDemoTemplate") KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaOperations(kafkaTemplate),
                (record, exception) -> new TopicPartition(properties.getDeadLetterTopic(), record.partition()));
        return new DefaultErrorHandler(recoverer,
                new FixedBackOff(properties.getRetryBackoffMs(), properties.getRetryMaxAttempts()));
    }

    @SuppressWarnings("unchecked")
    private KafkaOperations<Object, Object> kafkaOperations(KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate) {
        return (KafkaOperations<Object, Object>) (KafkaOperations<?, ?>) kafkaTemplate;
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

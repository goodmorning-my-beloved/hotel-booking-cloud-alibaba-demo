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
public class KafkaDemoConfig implements KafkaDemoTopology {

    public static final String DEMO_TOPIC = "hotel.kafka.demo.orders";
    public static final String DEMO_DLT_TOPIC = "hotel.kafka.demo.orders.DLT";
    public static final String PRIMARY_GROUP = "hotel-kafka-demo-primary";
    public static final String AUDIT_GROUP = "hotel-kafka-demo-audit";
    public static final String DLT_GROUP = "hotel-kafka-demo-dlt";

    @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}")
    private String bootstrapServers;

    @Bean
    NewTopic kafkaDemoTopic() {
        return TopicBuilder.name(DEMO_TOPIC)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "86400000")
                .build();
    }

    @Bean
    NewTopic kafkaDemoDeadLetterTopic() {
        return TopicBuilder.name(DEMO_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "604800000")
                .build();
    }

    @Bean
    ProducerFactory<String, KafkaDemoMessage> kafkaDemoProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
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
        factory.setConcurrency(2);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    DefaultErrorHandler kafkaDemoErrorHandler(
            @Qualifier("kafkaDemoTemplate") KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaOperations(kafkaTemplate),
                (record, exception) -> new TopicPartition(DEMO_DLT_TOPIC, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }

    @SuppressWarnings("unchecked")
    private KafkaOperations<Object, Object> kafkaOperations(KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate) {
        return (KafkaOperations<Object, Object>) (KafkaOperations<?, ?>) kafkaTemplate;
    }

    @Override
    public String demoTopic() {
        return DEMO_TOPIC;
    }

    @Override
    public String deadLetterTopic() {
        return DEMO_DLT_TOPIC;
    }

    @Override
    public String primaryConsumerGroup() {
        return PRIMARY_GROUP;
    }

    @Override
    public String auditConsumerGroup() {
        return AUDIT_GROUP;
    }

    @Override
    public String deadLetterConsumerGroup() {
        return DLT_GROUP;
    }

    @Override
    public List<String> demoTopics() {
        return List.of(DEMO_TOPIC, DEMO_DLT_TOPIC);
    }

    @Override
    public List<String> demoConsumerGroups() {
        return List.of(PRIMARY_GROUP, AUDIT_GROUP, DLT_GROUP);
    }
}

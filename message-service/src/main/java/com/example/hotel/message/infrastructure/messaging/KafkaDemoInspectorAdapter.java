package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoInspector;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.domain.model.KafkaDemoConsumerLag;
import com.example.hotel.message.domain.model.KafkaDemoTopicInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaDemoInspectorAdapter implements KafkaDemoInspector {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoInspectorAdapter.class);
    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(3);

    private final KafkaDemoTopology topology;
    private final String bootstrapServers;

    public KafkaDemoInspectorAdapter(KafkaDemoTopology topology,
                                     @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}")
                                     String bootstrapServers) {
        this.topology = topology;
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public List<KafkaDemoTopicInfo> topics() {
        try (AdminClient admin = adminClient()) {
            Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions = admin
                    .describeTopics(topology.demoTopics())
                    .allTopicNames()
                    .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<ConfigResource, Config> configs = admin
                    .describeConfigs(topicConfigResources())
                    .all()
                    .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            return descriptions.values().stream()
                    .map(description -> new KafkaDemoTopicInfo(
                            description.name(),
                            description.partitions().size(),
                            description.partitions().stream()
                                    .map(partition -> partition.partition())
                                    .sorted()
                                    .toList(),
                            selectedConfigs(configs.get(topicConfigResource(description.name())))))
                    .sorted(Comparator.comparing(KafkaDemoTopicInfo::topic))
                    .toList();
        } catch (Exception exception) {
            log.warn("Failed to inspect Kafka topics: {}", exception.getMessage());
            return List.of();
        }
    }

    @Override
    public List<KafkaDemoConsumerLag> consumerLags() {
        List<KafkaDemoConsumerLag> result = new ArrayList<>();
        try (AdminClient admin = adminClient()) {
            for (String group : topology.demoConsumerGroups()) {
                List<TopicPartition> partitions = topicPartitions(admin, topicsForGroup(group));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                        latestOffsets(admin, partitions);
                Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin
                        .listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                for (TopicPartition partition : partitions) {
                    long endOffset = latestOffsets.get(partition).offset();
                    OffsetAndMetadata committed = committedOffsets.get(partition);
                    Long committedOffset = committed == null ? null : committed.offset();
                    Long lag = committedOffset == null ? null : Math.max(0, endOffset - committedOffset);
                    result.add(new KafkaDemoConsumerLag(
                            group,
                            partition.topic(),
                            partition.partition(),
                            committedOffset,
                            endOffset,
                            lag));
                }
            }
            return result.stream()
                    .sorted(Comparator
                            .comparing(KafkaDemoConsumerLag::consumerGroup)
                            .thenComparing(KafkaDemoConsumerLag::topic)
                            .thenComparingInt(KafkaDemoConsumerLag::partition))
                    .toList();
        } catch (Exception exception) {
            log.warn("Failed to inspect Kafka consumer lag: {}", exception.getMessage());
            return List.of();
        }
    }

    private AdminClient adminClient() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        return AdminClient.create(properties);
    }

    private List<String> topicsForGroup(String group) {
        if (topology.deadLetterConsumerGroup().equals(group)) {
            return List.of(topology.deadLetterTopic());
        }
        return List.of(topology.demoTopic());
    }

    private List<TopicPartition> topicPartitions(AdminClient admin, List<String> topics) throws Exception {
        Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions = admin
                .describeTopics(topics)
                .allTopicNames()
                .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return descriptions.values().stream()
                .flatMap(description -> description.partitions().stream()
                        .map(partition -> new TopicPartition(description.name(), partition.partition())))
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .toList();
    }

    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets(AdminClient admin,
                                                                                       List<TopicPartition> partitions)
            throws Exception {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        partitions.forEach(partition -> request.put(partition, OffsetSpec.latest()));
        return admin.listOffsets(request)
                .all()
                .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private List<ConfigResource> topicConfigResources() {
        return topology.demoTopics().stream()
                .map(this::topicConfigResource)
                .toList();
    }

    private ConfigResource topicConfigResource(String topic) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topic);
    }

    private Map<String, String> selectedConfigs(Config config) {
        if (config == null) {
            return Map.of();
        }
        Map<String, String> selected = new LinkedHashMap<>();
        putIfPresent(selected, config, "cleanup.policy");
        putIfPresent(selected, config, "retention.ms");
        putIfPresent(selected, config, "segment.bytes");
        return selected;
    }

    private void putIfPresent(Map<String, String> selected, Config config, String key) {
        ConfigEntry entry = config.get(key);
        if (entry != null) {
            selected.put(key, entry.value());
        }
    }
}

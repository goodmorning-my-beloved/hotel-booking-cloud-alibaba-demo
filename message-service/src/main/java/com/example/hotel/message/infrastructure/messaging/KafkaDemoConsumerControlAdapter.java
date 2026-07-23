package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoConsumerControl;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class KafkaDemoConsumerControlAdapter implements KafkaDemoConsumerControl {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaDemoConsumerControlAdapter(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void pausePrimary() {
        primaryContainer().pause();
    }

    @Override
    public void resumePrimary() {
        primaryContainer().resume();
    }

    @Override
    public boolean primaryPauseRequested() {
        return primaryContainer().isPauseRequested();
    }

    private MessageListenerContainer primaryContainer() {
        MessageListenerContainer container =
                registry.getListenerContainer(KafkaDemoConsumerAdapter.PRIMARY_LISTENER_ID);
        if (container == null) {
            throw new IllegalStateException("Kafka primary listener container is not registered");
        }
        return container;
    }
}

package org.example.kafka;

import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import static org.example.config.ProducerKafkaConfig.TEST_TOPIC;

@Service
public class MyEventListener {

    @KafkaListener(id = "forevent", topics = TEST_TOPIC)
    public void listen(@Payload String msg) {
        System.out.println("MyEventListener received message: " + msg);
    }

    // listens all @Kafkalisteners witn kafkaListenerContainerFactory
    // @EventListener
    @EventListener(condition = "event.listenerId.startsWith('forevent')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("eventHandler received event " + event);
    }


}

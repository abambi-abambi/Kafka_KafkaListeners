package org.example.kafka;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "multiListener", topics = MultiListener.MULTI_TOPIC, containerFactory = "integerListenerContainerFactory")
public class MultiListener {

    public static final String MULTI_TOPIC = "multi_topic";

    @KafkaHandler
    public void listen(String message) {
        System.out.println("MultiListener method 1 message [" + message + "]");
    }

    @KafkaHandler
    public void listen(Integer message) {
        System.out.println("MultiListener method 2 message [" + message + "]");
    }
}
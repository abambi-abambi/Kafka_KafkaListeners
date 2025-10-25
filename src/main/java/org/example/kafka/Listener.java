package org.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import static org.example.config.ProducerKafkaConfig.TEST_TOPIC;

@Component
public class Listener {
    @KafkaListener(id = "listen1", topics = TEST_TOPIC)
    public void listen(String message,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String kafkaKey
    ) {
        System.out.println("JustListener-1 listened: " + message + " from topic with partition key: " + kafkaKey);
    }
}
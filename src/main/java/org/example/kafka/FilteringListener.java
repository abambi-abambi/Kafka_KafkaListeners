package org.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static org.example.config.ValidatedConsumerConfig.VALID_TOPIC;

@Component
public class FilteringListener {

    // a filter can be set in properties in @Configuration ConsumerConfig

    @KafkaListener(id = "filteringListen", topics = VALID_TOPIC, filter = "recordFilterStrategy")
    public void listen(String message) {
        System.out.println("Filtering listener message: " + message);
    }
}

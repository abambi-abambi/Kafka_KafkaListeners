package org.example.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class Producer {
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final KafkaTemplate<Integer, Integer> integerKafkaTemplate;
    public void send(Integer key, String topic, String msg) {
        kafkaTemplate.send(topic, key, msg);
    }

    public void send(Integer key, String topic, Integer msg) {
        integerKafkaTemplate.send(topic, key, msg);
    }

}
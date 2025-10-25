package org.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.validation.annotation.Validated;

import static org.example.config.ValidatedConsumerConfig.VALID_TOPIC;

public class ValidatingListener {

    @KafkaListener(id = "validatingListener", topics = VALID_TOPIC, errorHandler = "kafkaListenerErrorHandler")
    public void listen(@Payload @Validated String message) {
        System.out.println("Validating listener: message " + message + "\"");
    }
}
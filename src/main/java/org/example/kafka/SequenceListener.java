package org.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static org.example.config.ProducerKafkaConfig.TEST_TOPIC;
import static org.example.kafka.MultiListener.MULTI_TOPIC;

@Component
public class SequenceListener {

//    See ContainerGroupSequencer - the sequence is set in it

    @KafkaListener(id = "group 1", topics = MULTI_TOPIC, containerGroup = "g1")
    public void listen1(String msg) {
        System.out.println("Listener group 1: " + msg);
    }

    @KafkaListener(id = "group 2", topics = MULTI_TOPIC, containerGroup = "g1")
    public void listen2(String msg) {
        System.out.println("Listener group 2: " + msg);
    }

    @KafkaListener(id = "group 3", topics = TEST_TOPIC, containerGroup = "g2")
    public void listen3(String msg) {
        System.out.println("Listener group 3: " + msg);
    }

    @KafkaListener(id = "group 4", topics = TEST_TOPIC, containerGroup = "g2")
    public void listen4(String msg) {
        System.out.println("Listener group 4: " + msg);
    }
}
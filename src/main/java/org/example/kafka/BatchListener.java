package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.example.config.ProducerKafkaConfig.TEST_TOPIC;

@Component
public class BatchListener {

    @KafkaListener(id = "batch", topics = TEST_TOPIC, containerFactory = "kafkaBatchListenerContainerFactory")
    public void listen(List<String> messages) {
        System.out.println("BatchListener-1: received number of messages - " + messages.size() + ",  listened messages: " + messages.toString());
//        GET thread name with pattern: bean_name-thread_number-C-listener_container_launch_number
        System.out.println("BatchListener-1: имя потока - " + Thread.currentThread().getName());
    }

    @KafkaListener(id="msg_batch", topics=TEST_TOPIC, containerFactory = "kafkaBatchListenerContainerFactory")
    public void listen2(List<Message> messages) {
        System.out.println("BatchListener-2  listened messages: " + messages.toString());
        System.out.println("BatchListener-2: имя потока - " + Thread.currentThread().getName());
    }

    @KafkaListener(id="consumer_batch", topics=TEST_TOPIC, containerFactory = "kafkaBatchListenerContainerFactory")
    public void listen3(List<ConsumerRecord<Integer, String>> consumerRecords) {
        System.out.println("BatchListener-3 listened consumerRecords: " + consumerRecords.toString());
        System.out.println("BatchListener-3 headers: " + consumerRecords.get(0).headers());
        System.out.println("BatchListener-3 records number: " + consumerRecords.size());
        System.out.println("BatchListener-3: имя потока - " + Thread.currentThread().getName());
    }

    @KafkaListener(id="consumer_batch_records", topics=TEST_TOPIC, containerFactory = "kafkaBatchListenerContainerFactory")
    public void listen4(ConsumerRecords<Integer, String> consumerRecords) {
        System.out.println("BatchListener-4 listened consumerRecords: " + consumerRecords.toString());
        System.out.println("BatchListener-4 partitions: " + consumerRecords.partitions());
        System.out.println("BatchListener-4 records : " + consumerRecords.records(TEST_TOPIC));
        System.out.println("BatchListener-4: имя потока - " + Thread.currentThread().getName());
    }
}

package org.example;

import lombok.var;
import org.example.config.ConsumerConfig;
import org.example.config.FilteringConsumerConfig;
import org.example.config.ProducerKafkaConfig;
import org.example.config.ValidatedConsumerConfig;
import org.example.kafka.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.time.LocalDateTime;

import static org.example.config.ProducerKafkaConfig.TEST_TOPIC;
import static org.example.config.ValidatedConsumerConfig.VALID_TOPIC;
import static org.example.kafka.MultiListener.MULTI_TOPIC;

/**
 * Hello world!
 */
public class App
{
    public static void main( String[] args ) throws InterruptedException {
        System.out.println( "Hello World!" );

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
                ConsumerConfig.class, ProducerKafkaConfig.class, Listener.class, BatchListener.class, MultiListener.class, SequenceListener.class, Producer.class,
                ValidatedConsumerConfig.class, ValidatingListener.class, Registry.class, FilteringConsumerConfig.class, FilteringListener.class,
                MyEventListener.class);

        Producer producer = context.getBean(Producer.class);

        producer.send(1, TEST_TOPIC, "Hello from App Spring Kafka - 1 " + LocalDateTime.now());
        producer.send(1, TEST_TOPIC,"Hello from Spring Kafka - 2 " + LocalDateTime.now());
        producer.send(1, TEST_TOPIC,"Hello from Spring Kafka - 3 " + LocalDateTime.now());
        Thread.sleep(6000);
        producer.send(0, MULTI_TOPIC, "Hello multi world! " + LocalDateTime.now());
//        producer.send(0, MULTI_TOPIC, 243545642);

        producer.send(0, VALID_TOPIC, "Hello validated world! " + LocalDateTime.now());
        producer.send(0, VALID_TOPIC, "Val");
        producer.send(0, VALID_TOPIC, "NEW Hello validated world! ");
        producer.send(0, VALID_TOPIC, "Hello validated world! ");
        producer.send(0, VALID_TOPIC, "NEW Hello validated world! ");

        var kafkaListenerEndpointRegistry = context.getBean(KafkaListenerEndpointRegistry.class);

        Registry registry = new Registry(kafkaListenerEndpointRegistry);
        registry.manage();
        Thread.sleep(1_000);
    }
}
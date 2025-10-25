package org.example.config;

import lombok.var;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerGroupSequencer;

import java.util.HashMap;
import java.util.Map;

import static org.example.kafka.MultiListener.MULTI_TOPIC;

@Configuration
// @EnableKafka is for kafka in Spring, not in SpringBoot
@EnableKafka
public class ConsumerConfig {

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("mytopic")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic multiTopic() {
        return TopicBuilder.name(MULTI_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // this default name of bean (kafkaListenerContainerFactory) is important for ordinary one-by-one listeners
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        // log event about container without receiving messages
        factory.getContainerProperties().setIdleEventInterval(1000L);

        return factory;
    }

    @Bean(name = "kafkaBatchListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaBatchListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setBatchListener(true);

        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumersConfig());
    }

    @Bean
    public Map<String, Object> consumersConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

//     ****

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, Integer>> integerListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, Integer> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory2());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setBatchListener(true);

        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, Integer> consumerFactory2() {
        return new DefaultKafkaConsumerFactory<>(consumersConfig2());
    }

    @Bean
    public Map<String, Object> consumersConfig2() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);

        return props;
    }

    /*
    ** works with kafka >= 2.7.3
     */
    @Bean
    ContainerGroupSequencer containerGroupSequencer(KafkaListenerEndpointRegistry kafkaEndpointEndpointRegistry) {
        var sequencer = new ContainerGroupSequencer(kafkaEndpointEndpointRegistry, 5000, "g1", "g2");
        sequencer.setStopLastGroupWhenIdle(true); // because last group doesn't stop itself by default
        return sequencer;
    }
}
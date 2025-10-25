package org.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.util.HashSet;
import java.util.Set;

@Configuration
@EnableKafka
public class FilteringConsumerConfig {
    public static final Set<String> MESSAGE_CASH = new HashSet<>();

    @Bean
    public RecordFilterStrategy<Integer, String> recordFilterStrategy() {
        System.out.println("Set filtering...");
        return consumerRecord -> {
            if (MESSAGE_CASH.contains(consumerRecord.value())) {
                System.out.println("Filtering: reject message " + consumerRecord.value());
                return true;
            } else {
                MESSAGE_CASH.add(consumerRecord.value());
                System.out.println("Filtering: accept message " + consumerRecord.value());
                return false;
            }
        };
    }
}
package org.example.kafka;

import lombok.RequiredArgsConstructor;
import lombok.var;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

/**
 * KafkaListenerEndpointRegistry manages @EnableKafka containers
 */
@RequiredArgsConstructor
@Service
public class Registry {
    private final KafkaListenerEndpointRegistry registry;

    public void manage() {
        var registryContainers = registry.getListenerContainers();
        int numOfRegistryContainers = registryContainers.size();

        System.out.println("numOfContainers @EnableKafka: " + numOfRegistryContainers);
        System.out.println("@EnableKafka containers: " + registryContainers);

        var allContainers = registry.getAllListenerContainers();
        int numOfAllContainers = allContainers.size();

        System.out.println("numOf all containers @EnableKafka: " + numOfAllContainers);
        System.out.println("@EnableKafka all containers: " + allContainers);

        registry.getListenerContainer("group 1").start();
    }
}
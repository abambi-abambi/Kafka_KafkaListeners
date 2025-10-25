package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Configuration
@EnableKafka
public class ValidatedConsumerConfig implements KafkaListenerConfigurer {

    public static final String VALID_TOPIC = "validate_topic";

    @Bean
    public NewTopic validatingTopic() {
        return TopicBuilder.name(VALID_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        System.out.println("Validation beginning...");
        registrar.setValidator(new Validator() {

            @Override
            public boolean supports(Class<?> clazz) {
                System.out.println("Check input message type...");
//  check String as parameter of this Demo-project
                return clazz.equals(String.class);
            }

            @Override
            public void validate(Object target, Errors errors) {
                System.out.println("Validating...");
                if (target.toString().length() < 5 || target.toString().isEmpty()) {
                    System.out.println("String [" + target.toString() + "] length validation failed!");
                    errors.reject("Too short", "Message length should be >= 5 symbols");
                }
            }
        });
    }

    /**
     * for managing value of "errors.reject"
     */
    @Bean
    public KafkaListenerErrorHandler kafkaListenerErrorHandler() {
        return (message, exception) -> {
            System.out.println("Fail validation for message: " + message);
            System.out.println("Fail validation exception: " + exception.getMessage());
            return message;
        };
    }
}
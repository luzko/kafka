package com.epam.course.kafka.config;

import com.epam.course.kafka.model.VehicleSignal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private List<String> bootstrapAddress;

    @Value("${spring.kafka.group.tracker}")
    private String trackerId;

    @Value("${spring.kafka.group.logging}")
    private String loggingId;

    @Bean
    public ProducerFactory<Long, VehicleSignal> vehicleProducerFactory() {
        Map<String, Object> properties = configProperties(JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public ProducerFactory<Long, Double> distanceProducerFactory() {
        Map<String, Object> properties = configProperties(DoubleSerializer.class);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<Long, VehicleSignal> vehicleKafkaTemplate() {
        return new KafkaTemplate<>(vehicleProducerFactory());
    }

    @Bean
    public KafkaTemplate<Long, Double> distanceKafkaTemplate() {
        return new KafkaTemplate<>(distanceProducerFactory());
    }

    @Bean
    public ConsumerFactory<Long, VehicleSignal> distanceConsumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, trackerId);
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, VehicleSignal.class.getPackage().getName());
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, VehicleSignal> distanceKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, VehicleSignal> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(distanceConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, Double> loggingConsumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, loggingId);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Double> loggingKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, Double> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(loggingConsumerFactory());
        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic inputTopic(@Value("${spring.kafka.topic.input}") String inputTopic) {
        return TopicBuilder.name(inputTopic)
            .partitions(3)
            .replicas(2)
            .compact()
            .build();
    }

    @Bean
    public NewTopic outputTopic(@Value("${spring.kafka.topic.output}") String outputTopic) {
        return TopicBuilder.name(outputTopic)
            .partitions(3)
            .replicas(2)
            .compact()
            .build();
    }

    private Map<String, Object> configProperties(Object valueSerializer) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }
}

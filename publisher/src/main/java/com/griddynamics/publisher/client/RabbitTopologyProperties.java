package com.griddynamics.publisher.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rabbit.topology.basic")
public record RabbitTopologyProperties(String exchangeName, String queueName, String routingKey) {
}

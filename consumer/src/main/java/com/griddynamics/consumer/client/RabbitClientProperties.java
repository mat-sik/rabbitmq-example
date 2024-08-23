package com.griddynamics.consumer.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "rabbit")
public record RabbitClientProperties(String user, String password, String virtualHost, List<String> nodeAddresses) {
}

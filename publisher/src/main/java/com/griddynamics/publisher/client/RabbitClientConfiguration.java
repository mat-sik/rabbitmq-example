package com.griddynamics.publisher.client;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Configuration
public class RabbitClientConfiguration {

    @Bean
    public Connection rabbitMqConnection(RabbitClientProperties properties) throws IOException, TimeoutException {
        var factory = new ConnectionFactory();

        String user = properties.user();
        factory.setUsername(user);

        String password = properties.password();
        factory.setPassword(password);

        String virtualHost = properties.virtualHost();
        factory.setVirtualHost(virtualHost);

        List<String> nodeAddresses = properties.nodeAddresses();
        List<Address> addresses = getAddresses(nodeAddresses);

        return factory.newConnection(addresses, "publisher-client");
    }

    private static List<Address> getAddresses(List<String> hostnames) {
        return hostnames.stream()
                .map(RabbitClientConfiguration::getAddress)
                .toList();
    }

    private static Address getAddress(String hostname) {
        String[] values = hostname.split(":");
        if (values.length != 2) {
            throw new IllegalArgumentException("Incorrect hostname formatting provided in the application.yaml");
        }
        String host = values[0];
        int port = Integer.parseInt(values[1]);
        return new Address(host, port);
    }
}

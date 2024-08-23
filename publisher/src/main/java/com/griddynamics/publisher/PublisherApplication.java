package com.griddynamics.publisher;

import com.griddynamics.publisher.client.BasicPublisher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(BasicPublisher publisher) {
        return args -> {
            publisher.continuousPublish();
        };
    }

}
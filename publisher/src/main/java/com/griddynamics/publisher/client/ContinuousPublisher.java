package com.griddynamics.publisher.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Component
public class ContinuousPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousPublisher.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Duration RATE = Duration.ofSeconds(10);

    private static final String EXCHANGE_NAME = "exchange-direct";
    private static final String QUEUE_NAME = "queue-direct";
    private static final String ROUTING_KEY = "all";

    private final Connection connection;

    public ContinuousPublisher(Connection connection) {
        this.connection = connection;
    }

    public void continuousPublish() throws IOException, InterruptedException {
        Channel channel = initChannel();
        for (; ; ) {
            String dateTimeString = getCurrentDateTimeAsString();
            publishStringMessage(channel, dateTimeString);
            Thread.sleep(RATE.toMillis());
        }
    }

    public static void publishStringMessage(Channel channel, String message) throws IOException {
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        boolean mandatory = true;
        AMQP.BasicProperties properties = getProperties();
        channel.basicPublish(
                EXCHANGE_NAME,
                ROUTING_KEY,
                mandatory,
                properties,
                payload
        );
    }

    private static AMQP.BasicProperties getProperties() {
        int persistentDeliveryMode = 2;
        int noPriority = 0;
        return new AMQP.BasicProperties.Builder()
                .deliveryMode(persistentDeliveryMode)
                .priority(noPriority)
                .build();
    }

    public Channel initChannel() throws IOException {
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        channel.addConfirmListener((sequenceNumber, multiple) -> {
            LOGGER.info("Message with sequenceNumber: {} was confirmed. Multiple: {}", sequenceNumber, multiple);
        }, (sequenceNumber, multiple) -> {
            LOGGER.info("Message with sequenceNumber: {} was nack-ed. Multiple: {}", sequenceNumber, multiple);
        });

        ensureQuorumQueue(channel);

        return channel;
    }

    public static void ensureQuorumQueue(Channel channel) throws IOException {
        boolean durable = true;
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, durable);

        boolean exclusive = false;
        boolean autoDelete = false;
        channel.queueDeclare(QUEUE_NAME, durable, exclusive, autoDelete, Map.of("x-queue-type", "quorum"));
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
    }

    private String getCurrentDateTimeAsString() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return FORMATTER.format(currentDateTime);
    }
}

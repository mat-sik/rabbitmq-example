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

    private static final Duration RATE = Duration.ofMillis(100);

    private final Connection connection;
    private final String exchangeName;
    private final String queueName;
    private final String routingKey;

    public ContinuousPublisher(Connection connection, RabbitTopologyProperties topologyProperties) {
        this.connection = connection;
        this.exchangeName = topologyProperties.exchangeName();
        this.queueName = topologyProperties.queueName();
        this.routingKey = topologyProperties.routingKey();
    }

    public void continuousPublish() throws IOException, InterruptedException {
        Channel channel = initChannel();
        for (; ; ) {
            String dateTimeString = getCurrentDateTimeAsString();
            publishStringMessage(channel, dateTimeString);
            Thread.sleep(RATE.toMillis());
        }
    }

    public void publishStringMessage(Channel channel, String message) throws IOException {
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        boolean mandatory = true;
        AMQP.BasicProperties properties = getProperties();
        channel.basicPublish(
                exchangeName,
                routingKey,
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

    public void ensureQuorumQueue(Channel channel) throws IOException {
        boolean durable = true;
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, durable);

        boolean exclusive = false;
        boolean autoDelete = false;
        channel.queueDeclare(queueName, durable, exclusive, autoDelete, Map.of("x-queue-type", "quorum"));
        channel.queueBind(queueName, exchangeName, routingKey);
    }

    private String getCurrentDateTimeAsString() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return FORMATTER.format(currentDateTime);
    }
}

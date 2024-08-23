package com.griddynamics.publisher.client;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
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
public class BasicPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicPublisher.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Duration RATE = Duration.ofSeconds(10);

    private static final String EXCHANGE_NAME = "exchange-direct";
    private static final String QUEUE_NAME = "queue-direct";
    private static final String ROUTING_KEY = "all";

    private final Connection connection;

    public BasicPublisher(Connection connection) {
        this.connection = connection;
    }

    public void continuousPublish() throws IOException {
        Channel channel = connection.createChannel();
        channel.confirmSelect();

        ensureQuorumQueue(channel);

        while (true) {
            String dateTimeString = getCurrentDateTimeAsString();
            publishStringMessage(channel, dateTimeString);

            try {
                boolean ok = channel.waitForConfirms();
                if (!ok) {
                    LOGGER.error("Message has not been acked by broker.");
                }
                Thread.sleep(RATE.toMillis());
            } catch (InterruptedException ex) {
                LOGGER.error("Thread go interrupted during either waiting for ack or sleeping.");
            }
        }
    }

    public static void publishStringMessage(Channel channel, String message) throws IOException {
        channel.basicPublish(
                EXCHANGE_NAME,
                ROUTING_KEY,
                true, // mandatory
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes(StandardCharsets.UTF_8)
        );
    }

    public static void ensureQuorumQueue(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true); // durable
        // durable, non-exclusive, non-auto-delete, quorum queue
        channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of("x-queue-type", "quorum"));
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
    }

    private String getCurrentDateTimeAsString() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return FORMATTER.format(currentDateTime);
    }
}

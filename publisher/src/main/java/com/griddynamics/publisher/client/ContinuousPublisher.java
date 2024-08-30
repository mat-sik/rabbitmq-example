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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

@Component
public class ContinuousPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousPublisher.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Duration RATE = Duration.ofMillis(100);

    private static final String PUBLISHER_REDELIVERY_HEADER = "x-publisher-redelivery";

    private final Connection connection;

    private final String exchangeName;
    private final String queueName;
    private final String routingKey;

    private final Queue<Message> toPublish;
    private final ConcurrentNavigableMap<Long, byte[]> outstandingConfirms;

    public ContinuousPublisher(Connection connection, RabbitTopologyProperties topologyProperties) {
        this.connection = connection;

        this.exchangeName = topologyProperties.exchangeName();
        this.queueName = topologyProperties.queueName();
        this.routingKey = topologyProperties.routingKey();

        this.outstandingConfirms = new ConcurrentSkipListMap<>();
        this.toPublish = new ConcurrentLinkedQueue<>();
    }

    public void continuousPublish() throws IOException, InterruptedException {
        Channel channel = initChannel();

        for (; ; ) {
            String dateTimeString = getCurrentDateTimeAsString();

            byte[] payload = dateTimeString.getBytes(StandardCharsets.UTF_8);
            Message initialMessage = Message.MessageFactory.newInitialMessage(payload);

            toPublish.add(initialMessage);

            publishStringMessage(channel);
            Thread.sleep(RATE.toMillis());
        }
    }

    public void publishStringMessage(Channel channel) throws IOException {
        Message message = toPublish.poll();
        byte[] payload = message.payload();
        boolean isPublisherRedelivery = message.isPublisherRedelivery();

        long sequenceNumber = channel.getNextPublishSeqNo();
        outstandingConfirms.put(sequenceNumber, payload);

        AMQP.BasicProperties properties = getProperties(isPublisherRedelivery);

        boolean mandatory = true;
        channel.basicPublish(
                exchangeName,
                routingKey,
                mandatory,
                properties,
                payload
        );
    }

    private static AMQP.BasicProperties getProperties(boolean isPublisherRedelivery) {
        int persistentDeliveryMode = 2;
        int noPriority = 0;
        return new AMQP.BasicProperties.Builder()
                .headers(Map.of(PUBLISHER_REDELIVERY_HEADER, isPublisherRedelivery))
                .deliveryMode(persistentDeliveryMode)
                .priority(noPriority)
                .build();
    }

    public Channel initChannel() throws IOException {
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        channel.addConfirmListener((sequenceNumber, multiple) -> {
            cleanOutstandingConfirms(sequenceNumber, multiple);
            LOGGER.info("Message with sequenceNumber: {} was confirmed. Multiple: {}", sequenceNumber, multiple);
        }, (sequenceNumber, multiple) -> {
            republishMessages(sequenceNumber, multiple);
            LOGGER.info("Message with sequenceNumber: {} was nack-ed. Multiple: {}", sequenceNumber, multiple);
        });
        channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            LOGGER.info("Message returned. Reply Code: {}, Reply Text: {}, Exchange: {}, Routing Key: {}, Properties: {}, Body: {}",
                    replyCode,  // the code indicating why the message was returned
                    replyText,  // the human-readable reason for the return
                    exchange,   // the exchange to which the message was sent
                    routingKey, // the routing key used in the attempt to route the message
                    properties, // the AMQP properties of the message
                    new String(body) // the body of the message, converted to a string
            );
        });

        ensureQuorumQueue(channel);

        return channel;
    }

    public void republishMessages(long sequenceNumber, boolean multiple) {
        Function<byte[], Message> newRedeliveryMessage = Message.MessageFactory::newRedeliveryMessage;

        if (multiple) {
            boolean inclusive = true;
            ConcurrentNavigableMap<Long, byte[]> nacked = outstandingConfirms.headMap(
                    sequenceNumber, inclusive
            );
            nacked.forEach((_, redeliveryPayload) -> toPublish.add(newRedeliveryMessage.apply(redeliveryPayload)));
        } else {
            byte[] redeliveryPayload = outstandingConfirms.get(sequenceNumber);
            toPublish.add(newRedeliveryMessage.apply(redeliveryPayload));
        }
    }

    public void cleanOutstandingConfirms(long sequenceNumber, boolean multiple) {
        if (multiple) { // This is true, if all sequenceNumbers until this one were acked.
            boolean inclusive = true;
            ConcurrentNavigableMap<Long, byte[]> confirmed = outstandingConfirms.headMap(
                    sequenceNumber, inclusive
            );
            confirmed.clear();
        } else {
            outstandingConfirms.remove(sequenceNumber);
        }
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

package com.griddynamics.consumer.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class BasicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicConsumer.class);

    private static final String EXCHANGE_NAME = "exchange-direct";
    private static final String QUEUE_NAME = "queue-direct";
    private static final String ROUTING_KEY = "all";

    private final Connection connection;

    public BasicConsumer(Connection connection) {
        this.connection = connection;
    }

    public void continuousConsume() throws IOException {
        Channel channel = connection.createChannel();

        ensureQuorumQueue(channel);

        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck,
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException {
                        String routingKey = envelope.getRoutingKey();
                        String contentType = properties.getContentType();
                        long deliveryTag = envelope.getDeliveryTag();
                        boolean multiMessage = false;
                        channel.basicAck(deliveryTag, multiMessage);

                        LOGGER.info("Received message with routing key: {}, content type: {}, delivery tag: {}, body: {}",
                                routingKey, contentType, deliveryTag, new String(body, StandardCharsets.UTF_8));
                    }
                });
    }

    public static void ensureQuorumQueue(Channel channel) throws IOException {
        boolean durable = true;
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, durable);

        boolean exclusive = false;
        boolean autoDelete = false;
        channel.queueDeclare(QUEUE_NAME, durable, exclusive, autoDelete, Map.of("x-queue-type", "quorum"));
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
    }

}
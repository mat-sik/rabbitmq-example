package com.griddynamics.consumer.client;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class ContinuousConsumer {

    private static final String EXCHANGE_NAME = "exchange-direct";
    private static final String QUEUE_NAME = "queue-direct";
    private static final String ROUTING_KEY = "all";

    private static final int PREFETCH_AMOUNT = 100;

    private final Connection connection;

    public ContinuousConsumer(Connection connection) {
        this.connection = connection;
    }

    public void continuousConsume() throws IOException {
        Channel channel = connection.createChannel();
        channel.basicQos(PREFETCH_AMOUNT);

        ensureQuorumQueue(channel);

        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, new BatchConsumer(channel));
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

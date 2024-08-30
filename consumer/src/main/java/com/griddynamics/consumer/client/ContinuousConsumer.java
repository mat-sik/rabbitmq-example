package com.griddynamics.consumer.client;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class ContinuousConsumer {

    private final Connection connection;
    private final String exchangeName;
    private final String queueName;
    private final String routingKey;

    public ContinuousConsumer(Connection connection, RabbitTopologyProperties topologyProperties) {
        this.connection = connection;
        this.exchangeName = topologyProperties.exchangeName();
        this.queueName = topologyProperties.queueName();
        this.routingKey = topologyProperties.routingKey();
    }

    public void continuousConsume() throws IOException {
        Channel channel = connection.createChannel();

        ensureQuorumQueue(channel);

        boolean autoAck = false;
        channel.basicConsume(queueName, autoAck, new BasicConsumer(channel));
    }

    public void ensureQuorumQueue(Channel channel) throws IOException {
        boolean durable = true;
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, durable);

        boolean exclusive = false;
        boolean autoDelete = false;
        channel.queueDeclare(queueName, durable, exclusive, autoDelete, Map.of("x-queue-type", "quorum"));
        channel.queueBind(queueName, exchangeName, routingKey);
    }

}

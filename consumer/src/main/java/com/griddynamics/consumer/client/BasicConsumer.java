package com.griddynamics.consumer.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicConsumer extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicConsumer.class);

    public BasicConsumer(Channel channel) {
        super(channel);
    }

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
        getChannel().basicAck(deliveryTag, multiMessage);

        LOGGER.info("Received message with routing key: {}, content type: {}, delivery tag: {}, body: {}",
                routingKey, contentType, deliveryTag, new String(body, StandardCharsets.UTF_8));
    }

}

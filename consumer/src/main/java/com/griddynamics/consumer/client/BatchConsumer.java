package com.griddynamics.consumer.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class BatchConsumer extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumer.class);

    private static final String PUBLISHER_REDELIVERY_HEADER = "x-publisher-redelivery";
    private static final int ACK_THRESHOLD = 10;
    private static final Duration TIME_TO_ACK_THRESHOLD = Duration.ofSeconds(10);

    private long ackCounter;
    private long currentDeliveryTag;
    private long lastAckTime;

    private final Semaphore mutex;
    private boolean isTimedFlushRunning;

    public BatchConsumer(Channel channel) {
        super(channel);
        this.ackCounter = 0;
        this.currentDeliveryTag = -1;
        this.lastAckTime = -1;
        this.mutex = new Semaphore(1);
        this.isTimedFlushRunning = false;
    }

    @Override
    public void handleDelivery(
            String consumerTag,
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body
    ) throws IOException {
        consumeMessage(envelope, properties, body);
        ackMessage(envelope.getDeliveryTag());
    }

    public void consumeMessage(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();
        boolean isRedeliver = envelope.isRedeliver();

        // Retrieve the custom header from the properties
        boolean isPublisherRedelivery = isPublisherRedelivery(properties);

        // Log message details including the custom header
        LOGGER.info("Received message with routing key: {}, content type: {}, delivery tag: {}, redelivered: {}, isPublisherRedelivery: {}, body: {}",
                routingKey, contentType, deliveryTag, isRedeliver, isPublisherRedelivery, new String(body, StandardCharsets.UTF_8));
    }

    public void ackMessage(long deliveryTag) throws IOException {
        try {
            mutex.acquire();
            ensureTimedFlushRunning();
            lastAckTime = System.currentTimeMillis();
            currentDeliveryTag = deliveryTag;
            ackCounter++;
            if (ackCounter == ACK_THRESHOLD) {
                flushAck(currentDeliveryTag);
            }
            mutex.release();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void ensureTimedFlushRunning() {
        if (!isTimedFlushRunning) {
            Thread.ofVirtual().start(this::timedFlush);
            isTimedFlushRunning = true;
        }
    }

    public void timedFlush() {
        for (; ; ) {
            try {
                mutex.acquire();
                // If there is nothing to ack, stop virtual thread
                if (lastAckTime == -1) {
                    isTimedFlushRunning = false;
                    mutex.release();
                    break;
                }
                if (isFlushTime(lastAckTime)) {
                    flushAck(currentDeliveryTag);
                    mutex.release();
                } else {
                    mutex.release();
                    Thread.sleep(TIME_TO_ACK_THRESHOLD);
                }
            } catch (InterruptedException | IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public boolean isFlushTime(long lastConsumeTime) {
        long elapsed = System.currentTimeMillis() - lastConsumeTime;
        return elapsed >= TIME_TO_ACK_THRESHOLD.toMillis();
    }

    public void flushAck(long deliveryTag) throws IOException {
        boolean multiple = true;
        getChannel().basicAck(deliveryTag, multiple);
        ackCounter = 0;
        lastAckTime = -1;
    }

    public static boolean isPublisherRedelivery(AMQP.BasicProperties properties) {
        Map<String, Object> headers = properties.getHeaders();
        if (headers == null) {
            throw new RuntimeException("for some reason headers are null");
        }
        Object value = headers.get(PUBLISHER_REDELIVERY_HEADER);
        if (value == null) {
            throw new RuntimeException("failed to parse header value");
        }
        if (value instanceof Boolean isPublisherRedelivery) {
            return isPublisherRedelivery;
        } else {
            throw new RuntimeException("failed to parse header value");
        }
    }
}

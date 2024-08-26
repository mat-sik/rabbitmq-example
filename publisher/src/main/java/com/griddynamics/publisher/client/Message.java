package com.griddynamics.publisher.client;

public record Message(byte[] payload, boolean isPublisherRedelivery) {

    public static class MessageFactory {
        public static Message newInitialMessage(byte[] payload) {
            return new Message(payload, false);
        }

        public static Message newRedeliveryMessage(byte[] payload) {
            return new Message(payload, true);
        }
    }
}

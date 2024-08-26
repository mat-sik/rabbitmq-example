package com.griddynamics.publisher.client;

public record Message(String payload, boolean isPublisherRedelivery) {
}

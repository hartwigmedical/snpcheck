package com.hartwig.snpcheck.turquoise;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface TurquoiseEvent {
    Logger LOGGER = LoggerFactory.getLogger(TurquoiseEvent.class);

    String eventType();

    List<Subject> subjects();

    List<Label> labels();

    @Value.Default
    default ZonedDateTime timestamp() {
        return ZonedDateTime.now();
    }

    Publisher publisher();

    default void publish() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.registerModule(new Jdk8Module());
            LOGGER.info("Publishing message to Turquoise [{}]", this);
            ApiFuture<String> future = publisher().publish(PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(objectMapper.writeValueAsString(Event.of(timestamp(), eventType(), subjects(), labels()))))
                    .build());
            LOGGER.info("Message was published with id [{}]", future.get(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
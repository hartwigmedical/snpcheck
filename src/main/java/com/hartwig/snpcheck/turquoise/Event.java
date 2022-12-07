package com.hartwig.snpcheck.turquoise;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.time.ZonedDateTime;
import java.util.List;

@Value.Immutable
@JsonSerialize(as = ImmutableEvent.class)
public interface Event {

    @Value.Parameter
    ZonedDateTime timestamp();

    @Value.Parameter
    String type();

    @Value.Parameter
    List<Subject> subjects();

    @Value.Parameter
    List<Label> labels();

    static Event of(final ZonedDateTime timestamp, final String eventType, final List<Subject> subject, final List<Label> labels) {
        return ImmutableEvent.of(timestamp, eventType, subject, labels);
    }
}
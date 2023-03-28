package com.hartwig.snpcheck.turquoise;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSnpCheckEvent.class)
public interface SnpCheckEvent extends TurquoiseEvent {

    String sample();

    String result();

    default String eventType() {
        return "snpcheck.completed";
    }

    default List<Label> labels() {
        return List.of(Label.of("result", result()));
    }

    default List<Subject> subjects() {
        return List.of(Subject.of("sample", sample(), Collections.emptyList()));
    }

    static ImmutableSnpCheckEvent.Builder builder() {
        return ImmutableSnpCheckEvent.builder();
    }
}

package com.hartwig.snpcheck.turquoise;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.turquoise.Label;
import com.hartwig.pipeline.turquoise.Subject;
import com.hartwig.pipeline.turquoise.TurquoiseEvent;

import org.immutables.value.Value;

@Value.Immutable
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

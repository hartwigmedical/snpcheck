package com.hartwig.snpcheck.turquoise;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonSerialize(as = ImmutableSubject.class)
public interface Subject {

    @Value.Parameter
    String name();

    @Value.Parameter
    String type();

    @Value.Parameter
    List<Label> labels();

    static Subject of(final String type, final String name, final List<Label> labels) {
        return ImmutableSubject.of(name, type, labels);
    }
}

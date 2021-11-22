package org.embulk.output.http_json.units;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.embulk.config.ConfigException;

public enum RequestMode {
    DIRECT,
    BUFFERED;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static RequestMode fromString(String value) {
        return Stream.of(RequestMode.values())
                .filter(mode -> mode.toString().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(
                        () ->
                                new ConfigException(
                                        String.format(
                                                "Unknown mode: %s. Available modes are [%s].",
                                                value,
                                                Stream.of(RequestMode.values())
                                                        .map(RequestMode::toString)
                                                        .collect(Collectors.joining(", ")))));
    }
}

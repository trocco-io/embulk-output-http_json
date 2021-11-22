package org.embulk.output.http_json.units;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.embulk.config.ConfigException;

public enum HttpScheme {
    HTTP,
    HTTPS;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static HttpScheme fromString(String value) {
        return Stream.of(HttpScheme.values())
                .filter(mode -> mode.toString().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(
                        () ->
                                new ConfigException(
                                        String.format(
                                                "Unknown scheme: %s. Available schemes are [%s].",
                                                value,
                                                Stream.of(HttpScheme.values())
                                                        .map(HttpScheme::toString)
                                                        .collect(Collectors.joining(", ")))));
    }
}

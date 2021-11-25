package org.embulk.output.http_json.units;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.embulk.config.ConfigException;

public enum HttpMethod {
    GET,
    POST,
    PUT,
    HEAD,
    DELETE,
    PATCH;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static HttpMethod fromString(String value) {
        return Stream.of(HttpMethod.values())
                .filter(mode -> mode.toString().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(
                        () ->
                                new ConfigException(
                                        String.format(
                                                "Unknown method: %s. Available methods are [%s].",
                                                value,
                                                Stream.of(HttpMethod.values())
                                                        .map(HttpMethod::toString)
                                                        .collect(Collectors.joining(", ")))));
    }
}

package org.embulk.output.http_json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigSource;
import org.embulk.input.config.ConfigInputPlugin;
import org.embulk.output.http_json.extension.embulk.EmbulkExtension;
import org.embulk.output.http_json.extension.embulk.EmbulkTester;
import org.embulk.output.http_json.extension.wiremock.SaveRequestBodyResponseTransformer;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestHttpJsonOutputPlugin {

    @RegisterExtension
    static WireMockExtension wm =
            WireMockExtension.newInstance()
                    .options(
                            WireMockConfiguration.wireMockConfig()
                                    .dynamicPort()
                                    .extensions(new SaveRequestBodyResponseTransformer()))
                    .build();

    @RegisterExtension
    static EmbulkExtension embulk =
            EmbulkExtension.builder()
                    .registerPlugin(InputPlugin.class, "config", ConfigInputPlugin.class)
                    .registerPlugin(OutputPlugin.class, "http_json", HttpJsonOutputPlugin.class)
                    .build();

    @TempDir static Path tempDir;

    static final String TEST_PATH = "/test";

    @Test
    @SuppressWarnings("unchecked")
    public void test(EmbulkTester embulkTester) throws Throwable {
        final Path tempFile = Files.createFile(tempDir.resolve("test.txt"));
        final ConfigSource emptyOption = embulkTester.newConfigSource();

        wm.stubFor(
                WireMock.post(WireMock.urlPathEqualTo(TEST_PATH))
                        .willReturn(
                                WireMock.aResponse()
                                        .withStatus(200)
                                        .withBody("{\"message\": \"ok\"}")
                                        .withHeader("Content-Type", "application/json")
                                        .withTransformer(
                                                SaveRequestBodyResponseTransformer.NAME,
                                                SaveRequestBodyResponseTransformer
                                                        .OUTPUT_FILE_PATH_PARAMETER,
                                                tempFile.toString())));

        embulkTester.runOutput(
                embulkTester.loadFromYamlString(
                        String.join(
                                "\n",
                                "type: http_json",
                                "scheme: http",
                                "host: localhost",
                                "port: " + wm.getPort(),
                                "path: " + TEST_PATH,
                                "method: POST",
                                "transformer_jq: '{events: (.)}'")),
                schemaConfig(
                        columnConfig("s", Types.STRING, emptyOption),
                        columnConfig("i", Types.LONG, emptyOption),
                        columnConfig("f", Types.DOUBLE, emptyOption),
                        columnConfig("b", Types.BOOLEAN, emptyOption),
                        columnConfig("a", Types.JSON, emptyOption),
                        columnConfig("m", Types.JSON, emptyOption)),
                tasks(
                        records(
                                record("a", 5L, 5.5d, true, a(1L, 2L), m("a", "x")),
                                record("b", 6L, 5.6d, true, a(2L, 3L), m("a", "y")),
                                record("c", 7L, 5.7d, true, a(4L, 5L), m("a", "z"))),
                        records(
                                record("x", 8L, 5.8d, true, a(6L, 7L), m("a", "a")),
                                record("y", 9L, 5.9d, true, a(8L, 9L), m("a", "b")))));

        List<String> lines = Files.readAllLines(tempFile);
        Collections.sort(lines);
        assertEquals(2, lines.size());
        assertEquals(
                "{\"events\":[{\"s\":\"a\",\"i\":5,\"f\":5.5,\"b\":true,\"a\":[1,2],\"m\":{\"a\":\"x\"}},{\"s\":\"b\",\"i\":6,\"f\":5.6,\"b\":true,\"a\":[2,3],\"m\":{\"a\":\"y\"}},{\"s\":\"c\",\"i\":7,\"f\":5.7,\"b\":true,\"a\":[4,5],\"m\":{\"a\":\"z\"}}]}",
                lines.get(0));
        assertEquals(
                "{\"events\":[{\"s\":\"x\",\"i\":8,\"f\":5.8,\"b\":true,\"a\":[6,7],\"m\":{\"a\":\"a\"}},{\"s\":\"y\",\"i\":9,\"f\":5.9,\"b\":true,\"a\":[8,9],\"m\":{\"a\":\"b\"}}]}",
                lines.get(1));
    }

    @SuppressWarnings("unchecked")
    private List<List<List<Object>>> tasks(List<List<Object>>... tasks) {
        List<List<List<Object>>> result = new ArrayList<>();
        for (List<List<Object>> t : tasks) {
            result.add(t);
        }
        return Collections.unmodifiableList(result);
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> records(List<Object>... records) {
        List<List<Object>> builder = new ArrayList<>();
        for (List<Object> r : records) {
            builder.add(r);
        }
        return Collections.unmodifiableList(builder);
    }

    private List<Object> record(Object... values) {
        List<Object> builder = new ArrayList<>();
        for (Object v : values) {
            builder.add(v);
        }
        return Collections.unmodifiableList(builder);
    }

    private List<Object> a(Object... values) {
        List<Object> builder = new ArrayList<>();
        for (Object v : values) {
            builder.add(v);
        }
        return Collections.unmodifiableList(builder);
    }

    private Map<String, Object> m(Object... values) {
        Map<String, Object> builder = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            builder.put((String) values[i], values[i + 1]);
        }
        return Collections.unmodifiableMap(builder);
    }

    private SchemaConfig schemaConfig(ColumnConfig... columnConfigs) {
        List<ColumnConfig> builder = new ArrayList<>();
        for (ColumnConfig c : columnConfigs) {
            builder.add(c);
        }
        return new SchemaConfig(builder);
    }

    private ColumnConfig columnConfig(String name, Type type, ConfigSource option) {
        return new ColumnConfig(name, type, option);
    }
}

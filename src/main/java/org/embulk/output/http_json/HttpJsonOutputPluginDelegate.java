package org.embulk.output.http_json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.http_json.HttpJsonOutputPlugin.PluginTask;
import org.embulk.output.http_json.jackson.JacksonCommitWithFlushRecordBuffer;
import org.embulk.output.http_json.jackson.scope.JacksonAllInObjectScope;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.output.http_json.jq.JQ;
import org.embulk.spi.DataException;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSResponseReader;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpJsonOutputPluginDelegate
        implements RestClientOutputPluginDelegate<HttpJsonOutputPlugin.PluginTask> {

    private static final Logger logger =
            LoggerFactory.getLogger(HttpJsonOutputPluginDelegate.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BUFFER_ATTRIBUTE_KEY = "buf";
    private static final JQ jq = new JQ();

    @SuppressWarnings("unused")
    private final ConfigMapperFactory configMapperFactory;

    public HttpJsonOutputPluginDelegate(ConfigMapperFactory configMapperFactory) {
        this.configMapperFactory = configMapperFactory;
    }

    @Override
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount) {
        validateJsonQuery("transformer_jq", task.getTransformerJq());
        validateJsonQuery("retry_condition_jq", task.getRetryConditionJq());
        validateJsonQuery("success_condition_jq", task.getSuccessConditionJq());
    }

    private void validateJsonQuery(String name, String jqFilter) {
        try {
            jq.validateFilter(jqFilter);
        } catch (InvalidJQFilterException e) {
            throw new ConfigException(String.format("'%s' filter is invalid.", name), e);
        }
    }

    @Override
    public ServiceRequestMapper<? extends ValueLocator> buildServiceRequestMapper(PluginTask task) {
        return JacksonServiceRequestMapper.builder()
                .add(
                        new JacksonAllInObjectScope(
                                buildTimestampFormatter(task), task.getFillJsonNullForEmbulkNull()),
                        new JacksonTopLevelValueLocator(BUFFER_ATTRIBUTE_KEY))
                .build();
    }

    @Override
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex) {
        return new JacksonCommitWithFlushRecordBuffer(
                "responses",
                (records) -> {
                    final List<JsonNode> actualRecords =
                            records.map(r -> r.get(BUFFER_ATTRIBUTE_KEY))
                                    .collect(Collectors.toList());
                    final Function<List<JsonNode>, ObjectNode> bufferedRecordHandler =
                            (bufferedRecords) -> {
                                return requestWithRetry(
                                        task, buildBufferedBody(task, bufferedRecords));
                            };
                    return eachSlice(actualRecords, task.getBufferSize(), bufferedRecordHandler);
                });
    }

    @Override
    public ConfigDiff egestEmbulkData(
            PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports) {
        taskReports.forEach(report -> logger.info(report.toString()));
        return configMapperFactory.newConfigDiff();
    }

    private TimestampFormatter buildTimestampFormatter(PluginTask task) {
        return TimestampFormatter.builder(task.getDefaultTimestampFormat(), true)
                .setDefaultZoneFromString(task.getDefaultTimeZoneId())
                .setDefaultDateFromString(task.getDefaultDate())
                .build();
    }

    private <A, R> List<R> eachSlice(List<A> list, int sliceSize, Function<List<A>, R> function) {
        List<R> resultBuilder = new ArrayList<>();
        for (int i = 0; i < list.size(); i += sliceSize) {
            R result = function.apply(list.subList(i, Integer.min(i + sliceSize, list.size())));
            resultBuilder.add(result);
        }
        return Collections.unmodifiableList(resultBuilder);
    }

    private JsonNode buildBufferedBody(PluginTask task, List<JsonNode> records) {
        final ArrayNode an = OBJECT_MAPPER.createArrayNode();
        records.forEach(an::add);

        final List<JsonNode> out;
        try {
            out = jq.jq(task.getTransformerJq(), an);
        } catch (IllegalJQProcessingException e) {
            throw new DataException("Failed to apply 'transformer_jq'.", e);
        }
        if (out.size() != 1) {
            throw new DataException(
                    String.format(
                            "'transformer_jq' must return a single value. But %d values are returned.",
                            out.size()));
        }
        return out.get(0);
    }

    private JAXRSSingleRequester newJAXRSSingleRequester(PluginTask task, JsonNode json) {
        return new JAXRSSingleRequester() {

            @Override
            public javax.ws.rs.core.Response requestOnce(Client client) {
                return buildRequest(task, client, json);
            }

            @Override
            protected boolean isResponseStatusToRetry(javax.ws.rs.core.Response response) {
                return isMatchedResponse(
                        "retry_condition_jq",
                        task.getRetryConditionJq(),
                        transformResponseToObjectNode(response));
            }
        };
    }

    private ObjectNode transformResponseToObjectNode(javax.ws.rs.core.Response response) {
        ObjectNode responseJson = OBJECT_MAPPER.createObjectNode();
        responseJson.put("status_code", response.getStatus());
        responseJson.put("status_code_class", (response.getStatus() / 100) * 100);
        String json = response.readEntity(String.class);
        try {
            responseJson.set("response_body", OBJECT_MAPPER.readValue(json, ObjectNode.class));
        } catch (IOException e) {
            throw new DataException("Failed to parse response body.", e);
        }
        return responseJson;
    }

    private Boolean isMatchedResponse(String jqName, String jqFilter, ObjectNode responseJson) {
        final List<JsonNode> out;
        try {
            out = jq.jq(jqFilter, responseJson);
        } catch (IllegalJQProcessingException e) {
            throw new DataException("Failed to apply 'retry_condition_jq'.", e);
        }
        if (out.size() != 1) {
            throw new DataException(
                    String.format(
                            "'%s' must return a single value. But %d values are returned.",
                            jqName, out.size()));
        }
        JsonNode maybeBoolean = out.get(0);
        if (!maybeBoolean.isBoolean()) {
            throw new DataException(
                    String.format(
                            "'%s' must return a boolean value. But %s is returned.",
                            jqName, maybeBoolean.toString()));
        }
        return maybeBoolean.asBoolean();
    }

    private javax.ws.rs.core.Response buildRequest(
            PluginTask task, javax.ws.rs.client.Client client, final JsonNode json) {
        Entity<String> entity = Entity.entity(json.toString(), MediaType.APPLICATION_JSON);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        task.getHeaders().forEach(h -> h.forEach((k, v) -> headers.add(k, v)));
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        return client.target(buildEndpoint(task))
                .request()
                .headers(headers)
                .method(task.getMethod(), entity);
    }

    private <T> T tryWithJAXRSRetryHelper(PluginTask task, Function<JAXRSRetryHelper, T> f) {
        try (JAXRSRetryHelper retryHelper =
                new JAXRSRetryHelper(
                        task.getMaximumRetries(),
                        task.getInitialRetryIntervalMillis(),
                        task.getMaximumRetryIntervalMillis(),
                        new JAXRSClientCreator() {
                            @Override
                            public javax.ws.rs.client.Client create() {
                                return javax.ws.rs.client.ClientBuilder.newBuilder().build();
                            }
                        })) {
            return f.apply(retryHelper);
        }
    }

    private ObjectNode requestWithRetry(final PluginTask task, final JsonNode json) {
        return tryWithJAXRSRetryHelper(
                task,
                retryHelper -> {
                    return retryHelper.requestWithRetry(
                            newJAXRSResponseReader(task), newJAXRSSingleRequester(task, json));
                });
    }

    private JAXRSResponseReader<ObjectNode> newJAXRSResponseReader(PluginTask task) {
        return new JAXRSResponseReader<ObjectNode>() {

            private ObjectNode acceptAndReadOrThrow(javax.ws.rs.core.Response response) {
                ObjectNode responseJson = transformResponseToObjectNode(response);
                if (!isMatchedResponse(
                        "success_condition_jq", task.getSuccessConditionJq(), responseJson)) {
                    // TODO: Clone response to avoid to read the closed stream.
                    // TODO: Make it retryable.
                    throw new javax.ws.rs.WebApplicationException(response);
                }
                return responseJson;
            }

            @Override
            public ObjectNode readResponse(Response response) throws Exception {
                return acceptAndReadOrThrow(response);
            }
        };
    }

    private String buildEndpoint(PluginTask task) {
        StringBuilder endpointBuilder = new StringBuilder();
        endpointBuilder.append(task.getScheme().toString());
        endpointBuilder.append("://");
        endpointBuilder.append(task.getHost());
        task.getPort().ifPresent(port -> endpointBuilder.append(":").append(port));
        task.getPath().ifPresent(path -> endpointBuilder.append(path));
        return endpointBuilder.toString();
    }
}

package org.embulk.output.http_json;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.http_json.helpers.JacksonRequestRecordBuffer;
import org.embulk.output.http_json.units.HttpMethod;
import org.embulk.output.http_json.units.HttpScheme;
import org.embulk.output.http_json.units.RequestMode;
import org.embulk.spi.DataException;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.embulk.util.retryhelper.jaxrs.StringJAXRSResponseEntityReader;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpJsonOutputPluginDelegate
        implements RestClientOutputPluginDelegate<HttpJsonOutputPluginDelegate.PluginTask> {
    @Deprecated
    public interface PluginTask extends RestClientOutputTaskBase {
        @Config("scheme")
        @ConfigDefault("\"https\"")
        public HttpScheme getScheme();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("null")
        public Optional<Integer> getPort();

        @Config("path")
        @ConfigDefault("null")
        public Optional<String> getPath();

        @Config("headers")
        @ConfigDefault("{}")
        public List<Map<String, String>> getHeaders();

        @Deprecated
        @Config("request")
        @ConfigDefault("{}")
        public Request getRequest();

        @Deprecated
        @Config("response")
        @ConfigDefault("{}")
        public Response getResponse();

        @Config("maximum_retries")
        @ConfigDefault("7")
        public int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        public int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("60000")
        public int getMaximumRetryIntervalMillis();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        public String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        public String getDefaultDate();

        public String getEndpoint();

        public void setEndpoint(String endpoint);
    }

    @Deprecated
    public interface Request extends Task {
        @Config("method")
        @ConfigDefault("\"POST\"")
        public HttpMethod getMethod();

        @Config("mode")
        @ConfigDefault("\"buffered\"")
        public RequestMode getMode();

        @Config("fill_json_null_for_embulk_null")
        @ConfigDefault("false")
        public Boolean getFillJsonNullForEmbulkNull();

        @Config("buffered_body")
        @ConfigDefault("{}")
        public BufferedBody getBufferedBody();
    }

    @Deprecated
    public interface BufferedBody extends Task {
        @Config("buffer_size")
        @ConfigDefault("100")
        public Integer getBufferSize();

        @Config("root_pointer")
        @ConfigDefault("null")
        public Optional<String> getRootPointer();
    }

    @Deprecated
    public interface Response extends Task {
        @Config("success_condition")
        @ConfigDefault("null")
        public Optional<Condition> getSuccessCondition();

        @Config("retry_condition")
        @ConfigDefault("null")
        public Optional<Condition> getRetryCondition();
    }

    @Deprecated
    public interface Condition extends Task {
        @Config("status_codes")
        @ConfigDefault("null")
        public Optional<List<Integer>> getStatusCodes();

        @Config("messages")
        @ConfigDefault("null")
        public Optional<List<String>> getMessages();

        @Config("message_pointer")
        @ConfigDefault("/message")
        public String getMessagePointer();
    }

    private static final Logger logger =
            LoggerFactory.getLogger(HttpJsonOutputPluginDelegate.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BUFFER_ATTRIBUTE_KEY = "buf";

    @SuppressWarnings("unused")
    private final ConfigMapperFactory configMapperFactory;

    public HttpJsonOutputPluginDelegate(ConfigMapperFactory configMapperFactory) {
        this.configMapperFactory = configMapperFactory;
    }

    @Override
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount) {
        task.setEndpoint(buildEndpoint(task));
    }

    @Override
    public ServiceRequestMapper<? extends ValueLocator> buildServiceRequestMapper(PluginTask task) {
        final TimestampFormatter formatter =
                TimestampFormatter.builder(task.getDefaultTimestampFormat(), true)
                        .setDefaultZoneFromString(task.getDefaultTimeZoneId())
                        .setDefaultDateFromString(task.getDefaultDate())
                        .build();
        return JacksonServiceRequestMapper.builder()
                .add(
                        new JacksonAllInObjectScope(
                                formatter, task.getRequest().getFillJsonNullForEmbulkNull()),
                        new JacksonTopLevelValueLocator(BUFFER_ATTRIBUTE_KEY))
                .build();
    }

    @Override
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex) {
        return new JacksonRequestRecordBuffer(
                "responses",
                (records) -> {
                    switch (task.getRequest().getMode()) {
                        case BUFFERED:
                            return eachSlice(
                                    records.map(r -> r.get(BUFFER_ATTRIBUTE_KEY))
                                            .collect(Collectors.toList()),
                                    task.getRequest().getBufferedBody().getBufferSize(),
                                    slicedRecords ->
                                            requestWithRetry(
                                                    task, buildBufferedBody(task, slicedRecords)));
                        case DIRECT:
                            return records.map(r -> r.get(BUFFER_ATTRIBUTE_KEY))
                                    .map(json -> requestWithRetry(task, json))
                                    .collect(Collectors.toList());
                        default:
                            throw new ConfigException(
                                    "Unknown request mode: " + task.getRequest().getMode());
                    }
                });
    }

    @Override
    public ConfigDiff egestEmbulkData(
            PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports) {
        taskReports.forEach(report -> logger.info(report.toString()));
        return configMapperFactory.newConfigDiff();
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
        if (!task.getRequest().getBufferedBody().getRootPointer().isPresent()) {
            return an;
        }
        final ObjectNode root = OBJECT_MAPPER.createObjectNode();
        final JsonPointer jp =
                JsonPointer.compile(task.getRequest().getBufferedBody().getRootPointer().get());
        createNestedMissingNodes(root, jp, an);
        return root;
    }

    private <NodeType extends JsonNode> void createNestedMissingNodes(
            JsonNode json, JsonPointer jp, JsonNode leafValue) {
        final JsonNode parent = json.at(jp.head());
        if (parent.isArray()) {
            throw new DataException(
                    String.format(
                            "Unsupported data type of the value specified by Json Pointer: %s",
                            jp.head().toString()));
        }
        if (parent.isMissingNode()) {
            createNestedMissingNodes(json, jp.head(), OBJECT_MAPPER.createObjectNode());
        }
        ((ObjectNode) parent).set(jp.last().getMatchingProperty(), leafValue);
    }

    private String requestWithRetry(final PluginTask task, final JsonNode json) {
        return tryWithJAXRSRetryHelper(
                task,
                retryHelper -> {
                    return retryHelper.requestWithRetry(
                            new StringJAXRSResponseEntityReader(),
                            newJAXRSSingleRequester(task, json));
                });
    }

    private JAXRSSingleRequester newJAXRSSingleRequester(PluginTask task, JsonNode json) {
        return new JAXRSSingleRequester() {

            @Override
            public javax.ws.rs.core.Response requestOnce(Client client) {
                return buildRequest(task, client, json);
            }

            @Override
            protected boolean isResponseStatusToRetry(javax.ws.rs.core.Response response) {
                if (task.getResponse().getRetryCondition().isPresent()) {
                    return isMatchedResponse(
                            task.getResponse().getRetryCondition().get(), response);
                }
                return false;
            }
        };
    }

    private javax.ws.rs.core.Response buildRequest(
            PluginTask task, javax.ws.rs.client.Client client, final JsonNode json) {
        Entity<String> entity = Entity.entity(json.toString(), MediaType.APPLICATION_JSON);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        task.getHeaders().forEach(h -> h.forEach((k, v) -> headers.add(k, v)));
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        return client.target(task.getEndpoint())
                .request()
                .headers(headers)
                .method(task.getRequest().getMethod().name(), entity);
    }

    private Boolean isMatchedResponse(Condition cond, javax.ws.rs.core.Response response) {
        if (cond.getStatusCodes().isPresent()) {
            if (!cond.getStatusCodes().get().contains(response.getStatus())) {
                return false;
            }
        }
        if (cond.getMessages().isPresent()) {
            ObjectNode oj;
            try {
                oj = OBJECT_MAPPER.readValue(response.readEntity(String.class), ObjectNode.class);
            } catch (IOException e) {
                throw new DataException(e);
            }
            if (!cond.getMessages().get().contains(oj.get(cond.getMessagePointer()).asText())) {
                return false;
            }
        }
        return true;
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

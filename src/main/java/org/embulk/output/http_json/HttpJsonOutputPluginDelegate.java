package org.embulk.output.http_json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.client.ClientBuilder;
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
import org.embulk.output.http_json.jaxrs.JAXRSJsonNodeSingleRequester;
import org.embulk.output.http_json.jaxrs.JAXRSObjectNodeResponseEntityReader;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.output.http_json.jq.JQ;
import org.embulk.output.http_json.util.GlobalProgressLogger;
import org.embulk.output.http_json.validator.BeanValidator;
import org.embulk.spi.DataException;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
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

    private final ConfigMapperFactory configMapperFactory;

    private static GlobalProgressLogger globalProgressLogger;

    public HttpJsonOutputPluginDelegate(ConfigMapperFactory configMapperFactory) {
        this.configMapperFactory = configMapperFactory;
    }

    @Override
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount) {
        configureTask(task);
        BeanValidator.validate(task);
        validateJsonQuery("transformer_jq", task.getTransformerJq());
        validateJsonQuery("retryable_condition_jq", task.getRetryableConditionJq());
        validateJsonQuery("success_condition_jq", task.getSuccessConditionJq());
    }

    private void configureTask(PluginTask task) {
        globalProgressLogger = new GlobalProgressLogger(task.getLoggingInterval());
        globalProgressLogger.initializeLogger();
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
        globalProgressLogger.finish();
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
            long start = System.currentTimeMillis();
            R result = function.apply(list.subList(i, Integer.min(i + sliceSize, list.size())));
            globalProgressLogger.incrementRequestCount();
            globalProgressLogger.addElapsedTime(System.currentTimeMillis() - start);
            resultBuilder.add(result);
        }
        return Collections.unmodifiableList(resultBuilder);
    }

    private JsonNode buildBufferedBody(PluginTask task, List<JsonNode> records) {
        final ArrayNode an = OBJECT_MAPPER.createArrayNode();
        records.forEach(an::add);

        try {
            return jq.jqSingle(task.getTransformerJq(), an);
        } catch (IllegalJQProcessingException e) {
            throw new DataException("Failed to apply 'transformer_jq'.", e);
        }
    }

    private <T> T tryWithJAXRSRetryHelper(PluginTask task, Function<JAXRSRetryHelper, T> f) {
        try (JAXRSRetryHelper retryHelper =
                new JAXRSRetryHelper(
                        task.getMaximumRetries(),
                        task.getInitialRetryIntervalMillis(),
                        task.getMaximumRetryIntervalMillis(),
                        () -> ClientBuilder.newBuilder().build())) {
            return f.apply(retryHelper);
        }
    }

    private ObjectNode requestWithRetry(final PluginTask task, final JsonNode json) {
        return tryWithJAXRSRetryHelper(
                task,
                retryHelper ->
                        retryHelper.requestWithRetry(
                                JAXRSObjectNodeResponseEntityReader.newInstance(),
                                JAXRSJsonNodeSingleRequester.builder()
                                        .task(task)
                                        .requestBody(json)
                                        .build()));
    }
}

package org.embulk.output.http_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.output.http_json.units.HttpMethod;
import org.embulk.output.http_json.units.HttpScheme;
import org.embulk.output.http_json.units.RequestMode;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.timestamp.TimestampFormatter;

public class HttpJsonOutputPluginDelegate
        implements RestClientOutputPluginDelegate<HttpJsonOutputPluginDelegate.PluginTask> {
    public interface PluginTask extends RestClientOutputTaskBase {
        @Config("scheme")
        @ConfigDefault("\"https\"")
        public HttpScheme getString();

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
        public Map<String, String> getHeaders();

        @Config("request")
        @ConfigDefault("{}")
        public Request getRequest();

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
        String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();
    }

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

    public interface BufferedBody extends Task {
        @Config("buffer_size")
        @ConfigDefault("100")
        public Integer getBufferSize();

        @Config("root_pointer")
        @ConfigDefault("null")
        public Optional<String> getRootPointer();
    }

    public interface Response extends Task {
        @Config("success_condition")
        @ConfigDefault("null")
        public Optional<Condition> getSuccessCondition();
    }

    public interface Condition extends Task {
        @Config("status_codes")
        @ConfigDefault("null")
        public Optional<List<Integer>> getStatusCodes();

        @Config("messages")
        @ConfigDefault("null")
        public Optional<List<String>> getMessages();

        @Config("message_pointer")
        @ConfigDefault("null")
        public Optional<String> getMessagePointer();
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ConfigMapperFactory configMapperFactory;

    public HttpJsonOutputPluginDelegate(ConfigMapperFactory configMapperFactory) {
        this.configMapperFactory = configMapperFactory;
    }

    @Override
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount) {}

    @Override
    public ServiceRequestMapper<? extends ValueLocator> buildServiceRequestMapper(PluginTask task) {
        return null;
    }

    @Override
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex) {
        return null;
    }

    @Override
    public ConfigDiff egestEmbulkData(
            PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports) {
        return null;
    }
}

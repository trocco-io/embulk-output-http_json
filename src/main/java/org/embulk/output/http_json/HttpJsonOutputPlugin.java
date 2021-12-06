package org.embulk.output.http_json;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.embulk.base.restclient.RestClientOutputPluginBase;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapperFactory;

public class HttpJsonOutputPlugin
        extends RestClientOutputPluginBase<HttpJsonOutputPlugin.PluginTask> {
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

    public HttpJsonOutputPlugin() {
        super(
                CONFIG_MAPPER_FACTORY,
                HttpJsonOutputPlugin.PluginTask.class,
                new HttpJsonOutputPluginDelegate(CONFIG_MAPPER_FACTORY));
    }

    public interface PluginTask extends RestClientOutputTaskBase {
        @Config("scheme")
        @ConfigDefault("\"https\"")
        @Pattern(regexp = "^(https|http)$")
        public String getScheme();

        @Config("host")
        @NotBlank
        public String getHost();

        @Config("port")
        @ConfigDefault("null")
        public Optional<@Min(0) @Max(65535) Integer> getPort();

        @Config("path")
        @ConfigDefault("null")
        public Optional<@NotBlank String> getPath();

        @Config("headers")
        @ConfigDefault("[]")
        public List<@Size(min = 1, max = 1) Map<@NotBlank String, @NotBlank String>> getHeaders();

        @Config("method")
        @ConfigDefault("\"POST\"")
        @Pattern(
                regexp = "^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)$",
                flags = Pattern.Flag.CASE_INSENSITIVE)
        public String getMethod();

        @Config("buffer_size")
        @ConfigDefault("100")
        @Positive
        public Integer getBufferSize();

        @Config("fill_json_null_for_embulk_null")
        @ConfigDefault("false")
        @NotNull
        public Boolean getFillJsonNullForEmbulkNull();

        @Config("transformer_jq")
        @ConfigDefault("\".\"")
        @NotBlank
        public String getTransformerJq();

        @Config("success_condition_jq")
        @ConfigDefault("\".status_code_class == 200\"")
        @NotBlank
        public String getSuccessConditionJq();

        @Config("retryable_condition_jq")
        @ConfigDefault("\"true\"")
        @NotBlank
        public String getRetryableConditionJq();

        @Config("show_request_body_on_error")
        @ConfigDefault("true")
        @NotNull
        public Boolean getShowRequestBodyOnError();

        @Config("maximum_retries")
        @ConfigDefault("7")
        @PositiveOrZero
        public int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        @PositiveOrZero
        public int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("60000")
        @PositiveOrZero
        public int getMaximumRetryIntervalMillis();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        @NotBlank
        public String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        @NotNull
        public String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}$")
        public String getDefaultDate();
    }
}

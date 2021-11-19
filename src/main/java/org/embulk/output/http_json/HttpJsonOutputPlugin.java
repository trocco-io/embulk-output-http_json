package org.embulk.output.http_json;

import java.util.List;
import java.util.Optional;
import javax.validation.Validation;
import javax.validation.Validator;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;

public class HttpJsonOutputPlugin implements OutputPlugin {
    private static final Validator VALIDATOR =
            Validation.byProvider(ApacheValidationProvider.class)
                    .configure()
                    .buildValidatorFactory()
                    .getValidator();
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().withValidator(VALIDATOR).build();

    public interface PluginTask extends Task {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("option2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("option3")
        @ConfigDefault("null")
        public Optional<String> getOption3();
    }

    @Override
    public ConfigDiff transaction(
            ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.toTaskSource());
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(
            TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        throw new UnsupportedOperationException(
                "http_json output plugin does not support resuming");
    }

    @Override
    public void cleanup(
            TaskSource taskSource,
            Schema schema,
            int taskCount,
            List<TaskReport> successTaskReports) {}

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        // Write your code here :)
        throw new UnsupportedOperationException(
                "HttpJsonOutputPlugin.run method is not implemented yet");
    }
}

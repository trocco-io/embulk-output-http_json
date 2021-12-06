package org.embulk.output.http_json.extension.embulk;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.exec.BulkLoader;
import org.embulk.exec.ExecutionResult;
import org.embulk.spi.ExecSessionInternal;
import org.embulk.util.config.units.SchemaConfig;

public class EmbulkTester implements AutoCloseable {

    private final ExecSessionInternal execSessionInternal;
    private final EmbulkSystemProperties embulkSystemProperties;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    EmbulkTester(
            ExecSessionInternal execSessionInternal,
            EmbulkSystemProperties embulkSystemProperties) {
        this.execSessionInternal = execSessionInternal;
        this.embulkSystemProperties = embulkSystemProperties;
    }

    public ExecutionResult runOutput(
            ConfigSource outConfig, SchemaConfig inSchema, List<List<ConfigSource>> inData) {
        ConfigSource execConfig = newConfigSource().set("min_output_tasks", 1);
        ConfigSource inConfig =
                newConfigSource()
                        .set("type", "config")
                        .set("columns", inSchema)
                        .set("values", inData);
        ConfigSource config =
                newConfigSource().set("exec", execConfig).set("in", inConfig).set("out", outConfig);
        return exec(config);
    }

    private ExecutionResult exec(ConfigSource config) {
        if (closed.get()) {
            throw new IllegalStateException("EmbulkTester is already closed.");
        }
        return new BulkLoader(embulkSystemProperties).run(execSessionInternal, config);
    }

    @SuppressWarnings("deprecation")
    public ConfigSource newConfigSource() {
        return execSessionInternal.getModelManager().newConfigSource();
    }

    public ConfigSource loadFromYamlString(String yaml) {
        return new ConfigLoader(execSessionInternal.getModelManager()).fromYamlString(yaml);
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            execSessionInternal.cleanup();
        }
    }
}

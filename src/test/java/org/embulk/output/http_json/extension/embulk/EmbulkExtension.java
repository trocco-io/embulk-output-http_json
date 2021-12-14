package org.embulk.output.http_json.extension.embulk;

import java.util.LinkedHashMap;
import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ModelManager;
import org.embulk.deps.buffer.PooledBufferAllocatorImpl;
import org.embulk.exec.SimpleTempFileSpaceAllocator;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.ExecSessionInternal;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.TempFileSpaceAllocator;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class EmbulkExtension
        implements BeforeTestExecutionCallback, AfterTestExecutionCallback, ParameterResolver {

    public static class Builder {
        private Properties embulkSystemProperties = null;
        private LinkedHashMap<Class<?>, LinkedHashMap<String, Class<?>>> builtinPlugins;

        Builder() {
            this.builtinPlugins = new LinkedHashMap<>();
            this.builtinPlugins.put(DecoderPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(EncoderPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(ExecutorPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(FileInputPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(FileOutputPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(FilterPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(FormatterPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(GuessPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(InputPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(OutputPlugin.class, new LinkedHashMap<>());
            this.builtinPlugins.put(ParserPlugin.class, new LinkedHashMap<>());
        }

        public <T> Builder registerPlugin(
                final Class<T> iface, final String name, final Class<?> impl) {
            this.builtinPlugins.get(iface).put(name, impl);
            return this;
        }

        public <T> Builder setEmbulkSystemProperties(final Properties properties) {
            this.embulkSystemProperties = properties;
            return this;
        }

        public EmbulkExtension build() {
            return new EmbulkExtension(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final String EMBULK_EXTENSION_NAMESPACE_STRING = "embulkExtension";
    private static final String EMBULK_TESTER = "embulkTester";

    private final EmbulkSystemProperties embulkSystemProperties;
    private final LinkedHashMap<Class<?>, LinkedHashMap<String, Class<?>>> builtinPlugins;

    EmbulkExtension(final Builder builder) {
        this.builtinPlugins = builder.builtinPlugins;
        if (builder.embulkSystemProperties != null) {
            this.embulkSystemProperties = EmbulkSystemProperties.of(builder.embulkSystemProperties);
        } else {
            this.embulkSystemProperties = EmbulkSystemProperties.of(new Properties());
        }
    }

    // This constructor is invoked by JUnit Jupiter via reflection or ServiceLoader
    @SuppressWarnings("unused")
    public EmbulkExtension() {
        this(builder());
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        getStore(context).put(EMBULK_TESTER, newEmbulkTester());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        EmbulkTester embulkTester = getStore(context).remove(EMBULK_TESTER, EmbulkTester.class);
        if (embulkTester != null) {
            embulkTester.close();
            embulkTester = null;
        }
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(EmbulkTester.class);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return getStore(extensionContext).get(EMBULK_TESTER, EmbulkTester.class);
    }

    private Store getStore(ExtensionContext context) {
        return context.getStore(
                Namespace.create(
                        EMBULK_EXTENSION_NAMESPACE_STRING,
                        getClass(),
                        context.getRequiredTestMethod()));
    }

    @SuppressWarnings("unchecked")
    private EmbulkTester newEmbulkTester() {
        ExecSessionInternal.Builder builder =
                ExecSessionInternal.builderInternal(
                        newBufferAllocator(), newTempFileSpaceAllocator());
        builder.setEmbulkSystemProperties(this.embulkSystemProperties);
        builder.setModelManager(newModelManager());
        this.builtinPlugins
                .get(DecoderPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerDecoderPlugin(
                                        name, (Class<? extends DecoderPlugin>) impl));
        this.builtinPlugins
                .get(EncoderPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerEncoderPlugin(
                                        name, (Class<? extends EncoderPlugin>) impl));
        this.builtinPlugins
                .get(ExecutorPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerExecutorPlugin(
                                        name, (Class<? extends ExecutorPlugin>) impl));
        this.builtinPlugins
                .get(FileInputPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerFileInputPlugin(
                                        name, (Class<? extends FileInputPlugin>) impl));
        this.builtinPlugins
                .get(FileOutputPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerFileOutputPlugin(
                                        name, (Class<? extends FileOutputPlugin>) impl));
        this.builtinPlugins
                .get(FilterPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerFilterPlugin(
                                        name, (Class<? extends FilterPlugin>) impl));
        this.builtinPlugins
                .get(FormatterPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerFormatterPlugin(
                                        name, (Class<? extends FormatterPlugin>) impl));
        this.builtinPlugins
                .get(GuessPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerGuessPlugin(
                                        name, (Class<? extends GuessPlugin>) impl));
        this.builtinPlugins
                .get(InputPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerInputPlugin(
                                        name, (Class<? extends InputPlugin>) impl));
        this.builtinPlugins
                .get(OutputPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerOutputPlugin(
                                        name, (Class<? extends OutputPlugin>) impl));
        this.builtinPlugins
                .get(ParserPlugin.class)
                .forEach(
                        (name, impl) ->
                                builder.registerParserPlugin(
                                        name, (Class<? extends ParserPlugin>) impl));

        return new EmbulkTester(builder.build(), embulkSystemProperties);
    }

    private BufferAllocator newBufferAllocator() {
        // TODO: Respect this.embulkSystemProperties.getProperty("page_size")
        return PooledBufferAllocatorImpl.create();
    }

    private TempFileSpaceAllocator newTempFileSpaceAllocator() {
        return new SimpleTempFileSpaceAllocator();
    }

    @SuppressWarnings("deprecation")
    private ModelManager newModelManager() {
        return new ModelManager();
    }
}

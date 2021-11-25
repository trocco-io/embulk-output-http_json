package org.embulk.output.http_json;

import javax.validation.Validation;
import javax.validation.Validator;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.embulk.base.restclient.RestClientOutputPluginBase;
import org.embulk.util.config.ConfigMapperFactory;

public class HttpJsonOutputPlugin
        extends RestClientOutputPluginBase<HttpJsonOutputPluginDelegate.PluginTask> {
    private static final Validator VALIDATOR =
            Validation.byProvider(ApacheValidationProvider.class)
                    .configure()
                    .buildValidatorFactory()
                    .getValidator();
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().withValidator(VALIDATOR).build();

    public HttpJsonOutputPlugin() {
        super(
                CONFIG_MAPPER_FACTORY,
                HttpJsonOutputPluginDelegate.PluginTask.class,
                new HttpJsonOutputPluginDelegate(CONFIG_MAPPER_FACTORY));
    }
}

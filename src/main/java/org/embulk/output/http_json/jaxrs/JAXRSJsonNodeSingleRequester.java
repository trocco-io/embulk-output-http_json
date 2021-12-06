package org.embulk.output.http_json.jaxrs;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.embulk.config.ConfigException;
import org.embulk.output.http_json.HttpJsonOutputPlugin.PluginTask;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JAXRSJsonNodeSingleRequester extends JAXRSSingleRequester {

    private static final Logger logger =
            LoggerFactory.getLogger(JAXRSJsonNodeSingleRequester.class);

    public static class Builder {
        private PluginTask task;
        private JsonNode requestBody;

        private Builder() {}

        public Builder task(PluginTask task) {
            this.task = task;
            return this;
        }

        public Builder requestBody(JsonNode requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        public JAXRSJsonNodeSingleRequester build() {
            if (task == null || requestBody == null) {
                throw new IllegalStateException("task and requestBody must be set.");
            }
            return new JAXRSJsonNodeSingleRequester(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final JsonNode requestBody;
    private final String endpoint;
    private final String method;
    private final MultivaluedMap<String, Object> headers;
    private final JAXRSResponseJqCondition successCondition;
    private final JAXRSResponseJqCondition retryableCondition;
    private final boolean showRequestBodyOnError;

    private JAXRSJsonNodeSingleRequester(Builder builder) {
        this.requestBody = builder.requestBody;
        this.endpoint = buildEndpoint(builder.task);
        this.method = builder.task.getMethod();
        this.headers = buildHeaders(builder.task);
        try {
            this.successCondition =
                    new JAXRSResponseJqCondition(builder.task.getSuccessConditionJq());
            this.retryableCondition =
                    new JAXRSResponseJqCondition(builder.task.getRetryableConditionJq());
        } catch (InvalidJQFilterException e) {
            throw new ConfigException(e);
        }
        this.showRequestBodyOnError = builder.task.getShowRequestBodyOnError();
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

    private MultivaluedMap<String, Object> buildHeaders(PluginTask task) {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        task.getHeaders().forEach(h -> h.forEach((k, v) -> headers.add(k, v)));
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        return headers;
    }

    private Response doRequestOnce(Client client) {
        Entity<String> entity = Entity.entity(requestBody.toString(), MediaType.APPLICATION_JSON);
        Response delegate =
                client.target(endpoint).request().headers(headers).method(method, entity);
        return JAXRSEntityRecycleResponse.of(delegate);
    }

    @Override
    public Response requestOnce(Client client) {
        Response response = doRequestOnce(client);
        // NOTE: If an exception is thrown by the exception handling in the link below, the
        //       error message will be poor, so to avoid this, put the exception handling
        //       here.
        // https://github.com/embulk/embulk-util-retryhelper/blob/402412d/embulk-util-retryhelper-jaxrs/src/main/java/org/embulk/util/retryhelper/jaxrs/JAXRSRetryHelper.java#L107-L109
        try {
            if (!successCondition.isSatisfied(response)) {
                if (showRequestBodyOnError) {
                    logger.warn(
                            "Success condition is not satisfied. Condition jq:'{}', Request body: '{}'",
                            successCondition.getJqFilter(),
                            requestBody.toString());
                }
                throw JAXRSWebApplicationExceptionWrapper.wrap(response);
            }
        } catch (InvalidJQFilterException | IllegalJQProcessingException | IOException e) {
            try {
                String body = response.readEntity(String.class);
                throw new DataException("response_body: " + body, e);
            } catch (Exception e2) {
                logger.debug(
                        "Exception '{}' is thrown when reading the response.", e.getMessage(), e2);
                throw new DataException(e);
            }
        }
        return response;
    }

    @Override
    protected boolean isResponseStatusToRetry(Response response) {
        try {
            return retryableCondition.isSatisfied(response);
        } catch (InvalidJQFilterException | IllegalJQProcessingException | IOException e) {
            // TODO: Use a suitable exception class.
            throw new DataException(e);
        }
    }
}

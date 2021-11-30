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
import org.embulk.output.http_json.HttpJsonOutputPlugin.PluginTask;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;

public class JAXRSSingleRequesterBuilder {
    private JAXRSSingleRequesterBuilder() {}

    private PluginTask task;
    private JsonNode requestBody;

    public static JAXRSSingleRequesterBuilder builder() {
        return new JAXRSSingleRequesterBuilder();
    }

    public JAXRSSingleRequesterBuilder task(PluginTask task) {
        this.task = task;
        return this;
    }

    public JAXRSSingleRequesterBuilder requestBody(JsonNode requestBody) {
        this.requestBody = requestBody;
        return this;
    }

    public JAXRSSingleRequester build() {
        return new JAXRSSingleRequester() {

            @Override
            public Response requestOnce(Client client) {
                Response response = buildRequest(task, client, requestBody);
                response = new JAXRSReusableStringResponse(response);
                // NOTE: If an exception is thrown by the exception handling in the link below, the
                //       error message will be poor, so to avoid this, put the exception handling
                //       here.
                // https://github.com/embulk/embulk-util-retryhelper/blob/402412d/embulk-util-retryhelper-jaxrs/src/main/java/org/embulk/util/retryhelper/jaxrs/JAXRSRetryHelper.java#L107-L109
                if (response.getStatus() / 100 != 2) {
                    throw JAXRSWebApplicationExceptionWrapper.wrap(response);
                }
                return response;
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response) {
                try {
                    // TODO: Should use retry_condition, not success_condition
                    return !newJAXRSRequestSuccessCondition().test(response);
                } catch (InvalidJQFilterException | IllegalJQProcessingException | IOException e) {
                    // TODO: Use a suitable exception class.
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private JAXRSRequestSuccessCondition newJAXRSRequestSuccessCondition()
            throws InvalidJQFilterException {
        return new JAXRSRequestSuccessCondition(task.getSuccessConditionJq());
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

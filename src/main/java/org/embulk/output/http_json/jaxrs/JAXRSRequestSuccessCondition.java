package org.embulk.output.http_json.jaxrs;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import javax.ws.rs.core.Response;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.output.http_json.jq.JQ;

public class JAXRSRequestSuccessCondition {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final String jqFilter;
    private final JQ jq;

    public JAXRSRequestSuccessCondition(String jqFilter) {
        this.jqFilter = jqFilter;
        this.jq = new JQ();
    }

    private void validateFilter() throws InvalidJQFilterException {
        jq.validateFilter(jqFilter);
    }

    public boolean test(Response response)
            throws JsonParseException, JsonMappingException, IllegalJQProcessingException,
                    IOException, InvalidJQFilterException {
        validateFilter();
        return isSuccess(response);
    }

    private boolean isSuccess(Response response)
            throws JsonParseException, JsonMappingException, IOException,
                    IllegalJQProcessingException {
        final ObjectNode responseJson = transformResponseToObjectNode(response);
        return jq.jqBoolean(jqFilter, responseJson);
    }

    private ObjectNode transformResponseToObjectNode(Response response)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectNode responseJson = mapper.createObjectNode();
        responseJson.put("status_code", response.getStatus());
        responseJson.put("status_code_class", (response.getStatus() / 100) * 100);
        String json = response.readEntity(String.class);
        responseJson.set("response_body", mapper.readValue(json, ObjectNode.class));
        return responseJson;
    }
}

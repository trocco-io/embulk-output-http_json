package org.embulk.output.http_json.jaxrs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import javax.ws.rs.core.Response;

public class JAXRSResponseJson {
    private JAXRSResponseJson() {}

    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode convertResponseToObjectNode(Response response) throws IOException {
        ObjectNode responseJson = mapper.createObjectNode();
        responseJson.put("status_code", response.getStatus());
        responseJson.put("status_code_class", (response.getStatus() / 100) * 100);
        String json = response.readEntity(String.class);
        responseJson.set("response_body", mapper.readValue(json, JsonNode.class));
        return responseJson;
    }
}

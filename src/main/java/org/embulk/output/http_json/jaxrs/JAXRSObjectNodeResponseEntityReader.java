package org.embulk.output.http_json.jaxrs;

import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.ws.rs.core.Response;
import org.embulk.util.retryhelper.jaxrs.JAXRSResponseReader;

public class JAXRSObjectNodeResponseEntityReader implements JAXRSResponseReader<ObjectNode> {

    public static JAXRSObjectNodeResponseEntityReader newInstance() {
        return new JAXRSObjectNodeResponseEntityReader();
    }

    @Override
    public ObjectNode readResponse(Response response) throws Exception {
        return JAXRSResponseJson.convertResponseToObjectNode(response);
    }
}

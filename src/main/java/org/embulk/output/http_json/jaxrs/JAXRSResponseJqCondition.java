package org.embulk.output.http_json.jaxrs;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.embulk.output.http_json.jq.IllegalJQProcessingException;
import org.embulk.output.http_json.jq.InvalidJQFilterException;
import org.embulk.output.http_json.jq.JQ;

public class JAXRSResponseJqCondition {

    private final String jqFilter;
    private final JQ jq;

    public JAXRSResponseJqCondition(String jqFilter) throws InvalidJQFilterException {
        this.jqFilter = jqFilter;
        this.jq = new JQ();
        validateFilter();
    }

    private void validateFilter() throws InvalidJQFilterException {
        jq.validateFilter(jqFilter);
    }

    public boolean isSatisfied(Response response)
            throws InvalidJQFilterException, IOException, IllegalJQProcessingException {
        return jq.jqBoolean(jqFilter, JAXRSResponseJson.convertResponseToObjectNode(response));
    }
}

package org.embulk.output.http_json.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.embulk.base.restclient.jackson.JacksonTaskReportRecordBuffer;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;

public class JacksonRequestRecordBuffer extends JacksonTaskReportRecordBuffer {

    private static final ObjectMapper om = new ObjectMapper();
    private final String taskReportKeyName;
    private final Function<Stream<ObjectNode>, List<String>> requestResponseHandler;

    public JacksonRequestRecordBuffer(
            String taskReportKeyName,
            Function<Stream<ObjectNode>, List<String>> requestResponseHandler) {
        super(taskReportKeyName);
        this.taskReportKeyName = taskReportKeyName;
        this.requestResponseHandler = requestResponseHandler;
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(final TaskReport taskReport) {
        ArrayDeque<ObjectNode> records = forceToGetRecords();
        List<String> jsonList = this.requestResponseHandler.apply(records.stream());
        ArrayNode an = om.createArrayNode();
        for (String j : jsonList) {
            try {
                an.add(om.readValue(j, ObjectNode.class));
            } catch (IOException e) {
                throw new DataException(e);
            }
        }
        taskReport.set(this.taskReportKeyName, an);
        return taskReport;
    }

    @SuppressWarnings("unchecked")
    private ArrayDeque<ObjectNode> forceToGetRecords() {
        try {
            Class<?> klass = this.getClass().getSuperclass();
            Field field = klass.getDeclaredField("records");
            field.setAccessible(true);
            return field.get(this) == null ? null : (ArrayDeque<ObjectNode>) field.get(this);
        } catch (IllegalArgumentException
                | IllegalAccessException
                | NoSuchFieldException
                | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
}

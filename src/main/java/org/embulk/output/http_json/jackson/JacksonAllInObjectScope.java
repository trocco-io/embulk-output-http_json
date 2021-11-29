package org.embulk.output.http_json.jackson;

// NOTE: This file is a copy of the code in the link below, which allows you to handle arrays when
//       parsing JSON.
// https://github.com/embulk/embulk-base-restclient/blob/42e9f97/src/main/java/org/embulk/base/restclient/jackson/scope/JacksonAllInObjectScope.java

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.embulk.base.restclient.jackson.scope.JacksonObjectScopeBase;
import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;

public class JacksonAllInObjectScope extends JacksonObjectScopeBase {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TimestampFormatter timestampFormatter;
    private final boolean fillsJsonNullForEmbulkNull;

    public JacksonAllInObjectScope(
            final TimestampFormatter timestampFormatter, final boolean fillsJsonNullForEmbulkNull) {
        this.timestampFormatter = timestampFormatter;
        this.fillsJsonNullForEmbulkNull = fillsJsonNullForEmbulkNull;
    }

    @Override
    public ObjectNode scopeObject(final SinglePageRecordReader singlePageRecordReader) {
        final ObjectNode resultObject = OBJECT_MAPPER.createObjectNode();

        singlePageRecordReader
                .getSchema()
                .visitColumns(
                        new ColumnVisitor() {
                            @Override
                            public void booleanColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    resultObject.put(
                                            column.getName(),
                                            singlePageRecordReader.getBoolean(column));
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            @Override
                            public void longColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    resultObject.put(
                                            column.getName(),
                                            singlePageRecordReader.getLong(column));
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            @Override
                            public void doubleColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    resultObject.put(
                                            column.getName(),
                                            singlePageRecordReader.getDouble(column));
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            @Override
                            public void stringColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    resultObject.put(
                                            column.getName(),
                                            singlePageRecordReader.getString(column));
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            @Override
                            public void timestampColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    if (timestampFormatter == null) {
                                        resultObject.put(
                                                column.getName(),
                                                singlePageRecordReader
                                                        .getTimestamp(column)
                                                        .getEpochSecond());
                                    } else {
                                        resultObject.put(
                                                column.getName(),
                                                timestampFormatter.format(
                                                        singlePageRecordReader.getTimestamp(
                                                                column)));
                                    }
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            @Override
                            public void jsonColumn(final Column column) {
                                if (!singlePageRecordReader.isNull(column)) {
                                    try {
                                        resultObject.set(
                                                column.getName(),
                                                OBJECT_MAPPER.readTree(
                                                        singlePageRecordReader
                                                                .getJson(column)
                                                                .toJson()));
                                    } catch (IOException e) {
                                        throw new DataException("Failed to parse json value.", e);
                                    }
                                } else {
                                    resultObject.putNull(column.getName());
                                }
                            }
                        });
        return resultObject;
    }
}

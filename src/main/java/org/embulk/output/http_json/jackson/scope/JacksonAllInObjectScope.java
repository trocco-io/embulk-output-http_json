package org.embulk.output.http_json.jackson.scope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import org.embulk.base.restclient.jackson.scope.JacksonObjectScopeBase;
import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

// NOTE: This file is a copy of the code in the link below, which allows you to handle arrays when
//       parsing JSON.
// https://github.com/embulk/embulk-base-restclient/blob/42e9f97/src/main/java/org/embulk/base/restclient/jackson/scope/JacksonAllInObjectScope.java
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
                                    final Value v = singlePageRecordReader.getJson(column);
                                    final JsonNode j;
                                    try {
                                        j = valueToJsonNode(v);
                                    } catch (IOException e) {
                                        throw new DataException("Failed to parse json value.", e);
                                    }
                                    resultObject.set(column.getName(), j);
                                } else if (fillsJsonNullForEmbulkNull) {
                                    resultObject.putNull(column.getName());
                                }
                            }

                            private JsonNode valueToJsonNode(final Value v) throws IOException {
                                if (v.isArrayValue()) {
                                    return (ArrayNode) OBJECT_MAPPER.readTree(v.toJson());
                                } else if (v.isBinaryValue()) {
                                    return new TextNode(v.toJson());
                                } else if (v.isBooleanValue()) {
                                    return v.asBooleanValue().getBoolean()
                                            ? BooleanNode.TRUE
                                            : BooleanNode.FALSE;
                                } else if (v.isFloatValue()) {
                                    return new DoubleNode(v.asFloatValue().toDouble());
                                } else if (v.isIntegerValue()) {
                                    return new LongNode(v.asIntegerValue().toLong());
                                } else if (v.isMapValue()) {
                                    return (ObjectNode) OBJECT_MAPPER.readTree(v.toJson());
                                } else if (v.isNilValue()) {
                                    return NullNode.getInstance();
                                } else if (v.isNumberValue()) {
                                    return new DoubleNode(v.asNumberValue().toDouble());
                                } else if (v.isStringValue()) {
                                    return new TextNode(v.asStringValue().toString());
                                } else {
                                    return new TextNode(v.toJson());
                                }
                            }
                        });
        return resultObject;
    }
}

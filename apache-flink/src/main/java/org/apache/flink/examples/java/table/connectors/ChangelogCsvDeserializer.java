package org.apache.flink.examples.java.table.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.regex.Pattern;

public final class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final String columnDelimiter;

    public ChangelogCsvDeserializer(
            List<LogicalType> parsingTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo,
            String columnDelimiter) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        // return the type information required by Flink's core interfaces
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) {
        // converters must be opened
        converter.open(RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] message) {
        // parse the columns including a changelog flag
        final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
        final RowKind kind = RowKind.valueOf(columns[0]);
        final Row row = new Row(kind, parsingTypes.size());
        for (int i = 0; i < parsingTypes.size(); i++) {
            row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
        }
        // convert to internal data structure
        return (RowData) converter.toInternal(row);
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }
}

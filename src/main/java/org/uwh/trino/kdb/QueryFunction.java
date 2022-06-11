package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class QueryFunction extends AbstractConnectorTableFunction {
    private final KDBClient client;

    public QueryFunction(KDBClient client) {
        super("system",
                "query",
                List.of(ScalarArgumentSpecification.builder()
                .name("QUERY")
                .type(VARCHAR)
                .build()),
                GENERIC_TABLE);
        this.client = client;
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments) {
        ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
        String query = ((Slice) argument.getValue()).toStringUtf8();

        KDBTableHandle handle = null;
        try {
            handle = client.getTableHandle("", query);
            List<ColumnMetadata> columns = client.getTableMeta(handle);
            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(new QueryHandle(handle))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class QueryHandle implements ConnectorTableFunctionHandle
    {
        private final KDBTableHandle tableHandle;

        @JsonCreator
        public QueryHandle(@JsonProperty("tableHandle") KDBTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}

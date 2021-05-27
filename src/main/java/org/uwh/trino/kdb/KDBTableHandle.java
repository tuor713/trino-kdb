package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import javax.swing.text.html.Option;
import java.util.OptionalLong;

public class KDBTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;

    public KDBTableHandle(SchemaTableName name) {
        this.schemaName = name.getSchemaName();
        this.tableName = name.getTableName();
        this.constraint = TupleDomain.all();
        this.limit = OptionalLong.empty();
    }

    @JsonCreator
    public KDBTableHandle(@JsonProperty("schemaName") String schemaName,
                          @JsonProperty("tableName") String tableName,
                          @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
                          @JsonProperty("limit") OptionalLong limit) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.constraint = constraint;
        this.limit = limit;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() { return constraint; }

    @JsonProperty
    public OptionalLong getLimit() { return limit; }
}

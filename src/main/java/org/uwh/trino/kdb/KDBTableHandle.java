package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class KDBTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    private final boolean isPartitioned;
    // Year represented as yyyy, month as yyyy.MM, date as yyyy.MM.dd inline with KDB representation
    private final List<String> partitions;
    private final Optional<KDBColumnHandle> partitionColumn;

    @JsonCreator
    public KDBTableHandle(@JsonProperty("schemaName") String schemaName,
                          @JsonProperty("tableName") String tableName,
                          @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
                          @JsonProperty("limit") OptionalLong limit,
                          @JsonProperty("isPartitioned") boolean isPartitioned,
                          @JsonProperty("partitionColumn") Optional<KDBColumnHandle> partitionColumn,
                          @JsonProperty("partitions") List<String> partitions) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.constraint = constraint;
        this.limit = limit;
        this.isPartitioned = isPartitioned;
        this.partitionColumn = partitionColumn;
        this.partitions = partitions;
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

    @JsonProperty("isPartitioned")
    public boolean isPartitioned() {
        return isPartitioned;
    }

    @JsonProperty("partitions")
    public List<String> getPartitions() {
        return partitions;
    }

    @JsonProperty("partitionColumn")
    public Optional<KDBColumnHandle> getPartitionColumn() {
        return partitionColumn;
    }

    public boolean isQuery() {
        return isQuery(tableName);
    }

    public static boolean isQuery(String tableName) {
        return !tableName.matches("[a-zA-Z.][a-zA-Z._0-9]*");
    }

    @Override
    public String toString() {
        return "KDBTableHandle{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", constraint=" + constraint +
                ", limit=" + limit +
                ", isPartitioned=" + isPartitioned +
                ", partitions=" + partitions +
                ", partitionColumn=" + partitionColumn +
                '}';
    }
}

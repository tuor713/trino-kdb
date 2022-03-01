package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

public class KDBOutputTableHandle implements ConnectorInsertTableHandle, ConnectorOutputTableHandle {
    private final String namespace;
    private final String tableName;
    private final List<KDBColumnHandle> columns;

    @JsonCreator
    public KDBOutputTableHandle(@JsonProperty("namespace") String namespace, @JsonProperty("tableName") String tableName, @JsonProperty("columns") List<KDBColumnHandle> columns) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.columns = columns;
    }

    @JsonProperty("namespace")
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    public String getQualifiedTableName() {
        if (namespace.equals("")) {
            return tableName;
        } else {
            return "." + namespace + "." + tableName;
        }
    }

    @JsonProperty("columns")
    public List<KDBColumnHandle> getColumns() {
        return columns;
    }
}

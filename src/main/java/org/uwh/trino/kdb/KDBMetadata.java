package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KDBMetadata implements ConnectorMetadata {
    private static final String SCHEMA_NAME = "default";
    private final KDBClient client;

    public static class KDBColumnHandle implements ColumnHandle {
        final String name;
        final Type type;
        final KDBType kdbType;

        @JsonCreator
        public KDBColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") Type type, @JsonProperty("kdbType") KDBType kdbType) {
            this.name = name;
            this.type = type;
            this.kdbType = kdbType;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public Type getType() {
            return type;
        }

        @JsonProperty
        public KDBType getKdbType() { return kdbType; }
    }

    public KDBMetadata(KDBClient client) {
        this.client = client;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        try {
            return client.listTables().stream().map(t -> new SchemaTableName(SCHEMA_NAME, t)).collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        try {
            return client.listTables()
                    .stream()
                    .filter(t -> prefix.getTable().stream().allMatch(tname -> tname.equals(t)))
                    .map(t -> new SchemaTableName(SCHEMA_NAME, t))
                    .collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, client.getTableMeta(tableName.getTableName()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return columns.build();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return new KDBTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        KDBTableHandle handle = (KDBTableHandle) table;
        try {
            List<ColumnMetadata> columns = client.getTableMeta(handle.getTableName());
            return new ConnectorTableMetadata(new SchemaTableName(handle.getSchemaName(), handle.getTableName()), columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ImmutableMap.Builder<String,ColumnHandle> builder = ImmutableMap.builder();
        KDBTableHandle handle = (KDBTableHandle) tableHandle;
        try {
            List<ColumnMetadata> columns = client.getTableMeta(handle.getTableName());
            columns.forEach(col -> builder.put(col.getName(), new KDBColumnHandle(col.getName(), col.getType(), (KDBType) col.getProperties().get("kdb.type"))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        KDBColumnHandle handle = (KDBColumnHandle) columnHandle;
        return new ColumnMetadata(handle.name, handle.type);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        KDBTableHandle khandle = (KDBTableHandle) handle;
        TupleDomain<ColumnHandle> current = khandle.getConstraint();
        TupleDomain<ColumnHandle> next = current.intersect(constraint.getSummary());
        if (current.equals(next)) {
            return Optional.empty();
        }

        KDBTableHandle newHandle = new KDBTableHandle(khandle.getSchemaName(), khandle.getTableName(), next, khandle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(newHandle, constraint.getSummary()));
    }
}

package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Optional;

public class KDBColumnHandle implements ColumnHandle {
    private final String name;
    private final Type type;
    private final KDBType kdbType;
    private final Optional<KDBAttribute> attribute;
    private final boolean isPartitionColumn;

    @JsonCreator
    public KDBColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("kdbType") KDBType kdbType,
            @JsonProperty("attribute") Optional<KDBAttribute> attribute,
            @JsonProperty("isPartitionColumn") boolean isPartitionColumn) {
        this.name = name;
        this.type = type;
        this.kdbType = kdbType;
        this.attribute = attribute;
        this.isPartitionColumn = isPartitionColumn;
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

    @JsonProperty
    public Optional<KDBAttribute> getAttribute() {
        return attribute;
    }

    @JsonProperty("isPartitionColumn")
    public boolean isPartitionColumn() {
        return isPartitionColumn;
    }

    @Override
    public String toString() {
        return "KDBColumnHandle{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", kdbType=" + kdbType +
                ", attribute=" + attribute +
                '}';
    }
}

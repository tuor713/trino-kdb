package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

public class KDBSplit implements ConnectorSplit {
    private final String table;
    private final Optional<String> partition;

    @JsonCreator
    public KDBSplit(@JsonProperty("table") String table, @JsonProperty("partition") Optional<String> partition) {
        this.table = table;
        this.partition = partition;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public String getTable() {
        return table;
    }

    @JsonProperty("partition")
    public Optional<String> getPartition() {
        return partition;
    }
}

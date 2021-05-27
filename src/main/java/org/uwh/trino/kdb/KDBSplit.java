package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

public class KDBSplit implements ConnectorSplit {
    private final String table;

    @JsonCreator
    public KDBSplit(@JsonProperty("table") String table) {
        this.table = table;
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
}

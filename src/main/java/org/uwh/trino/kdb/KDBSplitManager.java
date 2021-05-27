package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

import java.util.List;

public class KDBSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, DynamicFilter dynamicFilter) {
        KDBTableHandle handle = (KDBTableHandle) table;
        return new FixedSplitSource(List.of(new KDBSplit(handle.getTableName())));
    }
}

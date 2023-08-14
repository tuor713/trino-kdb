package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class KDBSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
        KDBTableHandle handle = (KDBTableHandle) table;
        if (handle.isPartitioned()) {
            return new FixedSplitSource(
                    handle.getPartitions().stream().map(partition -> new KDBSplit(handle.getTableName(), Optional.of(partition))).collect(Collectors.toList())
            );
        } else {
            return new FixedSplitSource(List.of(new KDBSplit(handle.getTableName(), Optional.empty())));
        }
    }
}

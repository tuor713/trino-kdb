package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

public class KDBHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return KDBTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return KDBColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return KDBSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return KDBConnector.KDBTransactionHandle.class;
    }
}

package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

import java.util.List;

public class KDBPageSourceProvider implements ConnectorPageSourceProvider {
    private final KDBClient client;

    public KDBPageSourceProvider(KDBClient client) {
        this.client = client;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        KDBTableHandle tHandle = (KDBTableHandle) table;
        List<KDBMetadata.KDBColumnHandle> tColumns = (List) columns;

        return new KDBPageSource(client, tHandle, tColumns);
    }
}

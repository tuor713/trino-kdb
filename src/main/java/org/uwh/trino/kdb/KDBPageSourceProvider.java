package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

import java.util.List;

public class KDBPageSourceProvider implements ConnectorPageSourceProvider {
    private final KDBClient client;
    private final Config config;

    public KDBPageSourceProvider(KDBClient client, Config config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        KDBTableHandle tHandle = (KDBTableHandle) table;
        List<KDBColumnHandle> tColumns = (List) columns;

        return new KDBPageSource(client, config, tHandle, tColumns);
    }
}

package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

public class KDBPageSinkProvider implements ConnectorPageSinkProvider {
    private final KDBClient client;

    public KDBPageSinkProvider(KDBClient client, Config config) {
        this.client = client;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
        throw new UnsupportedOperationException("Table creation is not supported by KDB connector");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
        return new KDBPageSink(client, (KDBOutputTableHandle) insertTableHandle);
    }
}

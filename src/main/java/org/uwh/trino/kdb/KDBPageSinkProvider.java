package org.uwh.trino.kdb;

import io.trino.spi.connector.*;

public class KDBPageSinkProvider implements ConnectorPageSinkProvider {
    private final KDBClientFactory factory;

    public KDBPageSinkProvider(KDBClientFactory factory, Config config) {
        this.factory = factory;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
        throw new UnsupportedOperationException("Table creation is not supported by KDB connector");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
        return new KDBPageSink(factory.getClient(session.getIdentity()), (KDBOutputTableHandle) insertTableHandle, session.getProperty(Config.SESSION_INSERT_FUNCTION, String.class));
    }
}

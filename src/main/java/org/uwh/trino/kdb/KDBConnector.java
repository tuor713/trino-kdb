package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;

public class KDBConnector implements Connector {

    public enum KDBTransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private final KDBClient client;

    public KDBConnector(KDBClient client) {
        this.client = client;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return KDBTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return new KDBMetadata(client);
    }

    @Override
    public Optional<ConnectorHandleResolver> getHandleResolver() {
        return Optional.of(new KDBHandleResolver());
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new KDBSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new KDBPageSourceProvider(client);
    }
}

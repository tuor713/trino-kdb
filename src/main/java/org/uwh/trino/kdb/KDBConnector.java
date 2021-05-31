package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;

public class KDBConnector implements Connector {

    public enum KDBTransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private final KDBClient client;
    private final Config config;
    private final StatsManager statsManager;

    public KDBConnector(KDBClient client, Config config) {
        this.client = client;
        this.config = config;
        this.statsManager = new StatsManager(client);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return KDBTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return new KDBMetadata(client, config.useStats(), statsManager);
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
        return new KDBPageSourceProvider(client, config);
    }
}

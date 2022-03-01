package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;

public class KDBConnector implements Connector {

    public enum KDBTransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private final KDBClient client;
    private final Config config;
    private final StatsManager statsManager;
    private final KDBMetadata metadata;

    public KDBConnector(KDBClient client, Config config) {
        this.client = client;
        this.config = config;
        this.statsManager = new StatsManager(client);
        this.metadata = new KDBMetadata(client, config, statsManager);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return config.getSessionProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return KDBTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new KDBSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new KDBPageSourceProvider(client, config);
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return new KDBPageSinkProvider(client, config);
    }
}

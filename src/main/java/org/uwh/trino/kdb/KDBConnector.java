package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class KDBConnector implements Connector {

    public enum KDBTransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private final KDBClientFactory factory;
    private final Config config;
    private final StatsManager statsManager;
    private final KDBMetadata metadata;

    public KDBConnector(KDBClientFactory factory, Config config) {
        this.factory = factory;
        this.config = config;
        this.statsManager = new StatsManager(factory);
        this.metadata = new KDBMetadata(factory, config, statsManager);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return config.getSessionProperties();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return Set.of(new QueryFunction(factory, metadata));
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
        return new KDBPageSourceProvider(factory, config);
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return new KDBPageSinkProvider(factory, config);
    }
}

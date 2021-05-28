package org.uwh.trino.kdb;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;

public class KDBPageSource implements ConnectorPageSource {
    private KDBTableHandle table;
    private List<KDBMetadata.KDBColumnHandle> columns;
    private boolean finished = false;
    private final KDBClient client;
    private final Config config;
    private final int pageSize;
    private int currentPage = 0;

    public KDBPageSource(KDBClient client, Config config, KDBTableHandle table, List<KDBMetadata.KDBColumnHandle> columns) {
        this.table = table;
        this.columns = columns;
        this.client = client;
        this.config = config;
        this.pageSize = config.getPageSize();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getNextPage() {
        try {
            Page result = client.getData(table, columns, currentPage, pageSize);
            currentPage += 1;
            if (result == null || result.getPositionCount() < pageSize) {
                finished = true;
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {}
}

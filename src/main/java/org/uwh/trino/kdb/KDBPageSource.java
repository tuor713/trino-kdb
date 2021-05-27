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
    private int currentPage = 0;

    private static final long PAGE_SIZE = 50_000;

    public KDBPageSource(KDBClient client, KDBTableHandle table, List<KDBMetadata.KDBColumnHandle> columns) {
        this.table = table;
        this.columns = columns;
        this.client = client;
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
        finished = true;

        try {
            return client.getData(table, columns);
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

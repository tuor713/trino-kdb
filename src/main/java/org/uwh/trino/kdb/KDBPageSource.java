package org.uwh.trino.kdb;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;

public class KDBPageSource implements ConnectorPageSource {
    private KDBTableHandle table;
    private List<KDBColumnHandle> columns;
    private boolean finished = false;
    private final KDBClient client;
    private final int pageSize;
    private int currentPage = 0;
    private final boolean isVirtualTables;

    private long completedBytes = 0;
    private long readTimeNanos = 0;

    public KDBPageSource(KDBClient client, KDBTableHandle table, List<KDBColumnHandle> columns, int pageSize, boolean isVirtualTables) {
        this.table = table;
        this.columns = columns;
        this.client = client;
        this.pageSize = pageSize;
        this.isVirtualTables = isVirtualTables;
        if (table.getConstraint().isNone()) {
            finished = true;
        }
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }

        try {
            long nanos = System.nanoTime();
            Page result = client.getData(table, columns, currentPage, pageSize, isVirtualTables);
            readTimeNanos += (System.nanoTime() - nanos);
            completedBytes += result.getSizeInBytes();

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

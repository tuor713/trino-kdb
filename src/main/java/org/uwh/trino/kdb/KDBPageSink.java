package org.uwh.trino.kdb;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class KDBPageSink implements ConnectorPageSink {
    private final CompletableFuture NOT_BLOCKED = CompletableFuture.completedFuture(null);

    private final KDBClient client;
    private final KDBOutputTableHandle table;
    private final String insertFunction;

    public KDBPageSink(KDBClient client, KDBOutputTableHandle table, String insertFunction) {
        this.client = client;
        this.table = table;
        this.insertFunction = insertFunction;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        System.out.println("XXX: appendPage: "+page);

        try {
            client.writeData(table.getQualifiedTableName(), table.getColumns(), page, insertFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public void abort() {
    }
}

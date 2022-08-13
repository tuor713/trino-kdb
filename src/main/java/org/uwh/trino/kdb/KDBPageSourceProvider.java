package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class KDBPageSourceProvider implements ConnectorPageSourceProvider {
    private final KDBClientFactory factory;
    private final Config config;

    public KDBPageSourceProvider(KDBClientFactory factory, Config config) {
        this.factory = factory;
        this.config = config;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        KDBTableHandle tHandle = (KDBTableHandle) table;
        KDBSplit kSplit = (KDBSplit) split;
        List<KDBColumnHandle> tColumns = (List) columns;

        if (kSplit.getPartition().isPresent()) {
            tHandle = new KDBTableHandle(
                    tHandle.getNamespace(),
                    tHandle.getTableName(),
                    tHandle.getConstraint().intersect(getSplitColumnHandle(tHandle.getPartitionColumn().get(), kSplit.getPartition().get())),
                    tHandle.getLimit(),
                    tHandle.isPartitioned(),
                    tHandle.getPartitionColumn(),
                    tHandle.getPartitions(),
                    tHandle.getExtraFilters());
        }

        return new KDBPageSource(factory.getClient(session.getIdentity()), tHandle, tColumns, session.getProperty(Config.SESSION_PAGE_SIZE, Integer.class), session.getProperty(Config.SESSION_VIRTUAL_TABLES, Boolean.class));
    }

    private TupleDomain<ColumnHandle> getSplitColumnHandle(KDBColumnHandle partitionColumn, String partition) {
        if (partitionColumn.getKdbType() != KDBType.Date) {
            throw new UnsupportedOperationException("Only date partition key implemented yet");
        }

        long partitionValue = LocalDate.parse(partition, DateTimeFormatter.ofPattern("yyyy.MM.dd")).toEpochDay();
        return TupleDomain.withColumnDomains(Map.of(partitionColumn, Domain.singleValue(partitionColumn.getType(), partitionValue)));
    }
}

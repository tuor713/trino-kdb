package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateType;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class TestKDBSplits {
    KDBSplitManager sut = new KDBSplitManager();

    @Test
    public void testNonPartitionedTableSplits() throws Exception {
        ConnectorSplitSource splits = sut.getSplits(null, null,
                new KDBTableHandle("default", "table", TupleDomain.all(), OptionalLong.empty(), false, Optional.empty(), List.of()),
                ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING, DynamicFilter.EMPTY);
        assertEquals(noOfSplits(splits), 1);
    }

    @Test
    public void testPartitionedTableSplits() throws Exception {
        ConnectorSplitSource splits = sut.getSplits(null, null,
                new KDBTableHandle("default", "table", TupleDomain.all(), OptionalLong.empty(),
                        true,
                        Optional.of(new KDBColumnHandle("date", DateType.DATE, KDBType.Date, Optional.empty(), true)),
                        List.of("2021.05.28", "2021.05.29", "2021.05.30", "2021.05.31")),
                ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING, DynamicFilter.EMPTY);
        assertEquals(noOfSplits(splits), 4);
    }

    private int noOfSplits(ConnectorSplitSource source) throws Exception {
        return source.getNextBatch(NotPartitionedPartitionHandle.NOT_PARTITIONED, Integer.MAX_VALUE).get().getSplits().size();
    }
}

package org.uwh.trino.kdb;

import io.trino.metadata.TableHandle;
import io.trino.spi.connector.*;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKDBMetadata {
    @BeforeClass
    public static void init() throws Exception {
        kx.c connection = new kx.c("localhost", 8000, "user:password");
        TestKDBPlugin.initKDB(connection);
    }

    ConnectorSession session;
    KDBMetadata sut;

    @BeforeTest
    public void setup() throws Exception {
        session = TestingConnectorSession.builder().build();
        KDBClient client = new KDBClient("localhost", 8000, "user", "password");
        sut = new KDBMetadata(client, new Config(Map.of(Config.KDB_USE_STATS_KEY, "true")), new StatsManager(client));
    }

    @Test
    public void testListTables() {
        List<SchemaTableName> tables = sut.listTables(session, Optional.empty());

        // SchemaTableName always lower cases - no way to work around :(
        Set<String> expected = Set.of("atable", "btable", "ctable", "dtable", "keyed_table", "splay_table", "attribute_table", "partition_table", "casesensitivetable");

        assertEquals(tables.size(), expected.size());
        assertEquals(tables.stream().map(t -> t.getTableName()).collect(Collectors.toSet()), expected);
    }

    @Test
    public void testColumnAttributes() {
        ConnectorTableHandle handle = sut.getTableHandle(session, new SchemaTableName("default", "attribute_table"));
        Map<String, ColumnHandle> columns = sut.getColumnHandles(session, handle);

        assertEquals(((KDBColumnHandle) columns.get("parted_col")).getAttribute(), Optional.of(KDBAttribute.Parted));
        assertEquals(((KDBColumnHandle) columns.get("grouped_col")).getAttribute(), Optional.of(KDBAttribute.Grouped));
        assertEquals(((KDBColumnHandle) columns.get("sorted_col")).getAttribute(), Optional.of(KDBAttribute.Sorted));
        assertEquals(((KDBColumnHandle) columns.get("unique_col")).getAttribute(), Optional.of(KDBAttribute.Unique));
        assertEquals(((KDBColumnHandle) columns.get("plain_col")).getAttribute(), Optional.empty());
    }

    @Test
    public void testPartitionedTableMetadata() {
        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "partition_table"));
        Map<String, KDBColumnHandle> columns = (Map) sut.getColumnHandles(session, handle);

        assertTrue(columns.get("date").isPartitionColumn());
        assertTrue(handle.isPartitioned());
        assertEquals(handle.getPartitions(), List.of("2021.05.28", "2021.05.29", "2021.05.30", "2021.05.31"));
    }

    @Test
    public void testTableStats() throws Exception {
        ConnectorSession session = TestingConnectorSession.builder().build();
        KDBClient client = new KDBClient("localhost", 8000, "user", "password");
        KDBMetadata metadata = new KDBMetadata(client, new Config(Map.of(Config.KDB_USE_STATS_KEY, "false")), new StatsManager(client));
        TableStatistics stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")), Constraint.alwaysTrue());
        assertEquals(stats, TableStatistics.empty());

        metadata = new KDBMetadata(client, new Config(Map.of(Config.KDB_USE_STATS_KEY, "true")), new StatsManager(client));
        stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")), Constraint.alwaysTrue());
        assertEquals(stats.getRowCount().getValue(), 3.0, 0.1);
    }

    @Test
    public void testPartitionedTableStats() {
        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "partition_table"));
        TableStatistics stats = sut.getTableStatistics(session, handle, Constraint.alwaysTrue());
        assertEquals(stats.getRowCount().getValue(), 12.0, 0.1);
    }
}

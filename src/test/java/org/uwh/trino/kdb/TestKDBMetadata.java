package org.uwh.trino.kdb;

import io.trino.spi.connector.*;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

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
        kx.c connection = new kx.c("localhost", 8000, "user:password");
        connection.k(".trino.touch:1");
        Config cfg = new Config(Map.of(Config.KDB_USE_STATS_KEY, "true", Config.KDB_DYNAMIC_STATS, "true"));
        session = TestingConnectorSession.builder().setPropertyMetadata(cfg.getSessionProperties()).build();
        KDBClientFactory factory = new KDBClientFactory("localhost", 8000, "user", "password", Optional.empty(), Optional.empty());
        sut = new KDBMetadata(factory, cfg, new StatsManager(factory));
        // some time to allow metadata queries to finish
        Thread.sleep(50);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        kx.c connection = new kx.c("localhost", 8000, "user:password");
        connection.k("delete stats from `.trino");
    }

    @Test
    public void testListSchemas() {
        List<String> schemas = sut.listSchemaNames(session);
        Set<String> expected = Set.of("default", "myns", "casens");
        assertTrue(Set.copyOf(schemas).containsAll(expected));
    }

    @Test
    public void testListTables() {
        List<SchemaTableName> tables = sut.listTables(session, Optional.empty());

        // SchemaTableName always lower cases - no way to work around :(
        Set<String> expected = Set.of("atable", "btable", "ctable", "dtable", "keyed_table", "splay_table", "attribute_table", "partition_table", "casesensitivetable",
                "ltable", "lsource",
                "itable", "longitable", "caseitable", "ikeytable");

        assertEquals(tables.size(), expected.size());
        assertEquals(tables.stream().map(SchemaTableName::getTableName).collect(Collectors.toSet()), expected);
    }

    @Test
    public void testListTablesInSchema() {
        List<SchemaTableName> tables = sut.listTables(session, Optional.of("myns"));
        Set<String> expected = Set.of("atable", "btable", "ctable", "instable");
        assertEquals(tables.size(), expected.size());
        assertEquals(tables.stream().map(SchemaTableName::getTableName).collect(Collectors.toSet()), expected);
    }

    @Test
    public void testListTablesInCaseSensitiveSchema() {
        List<SchemaTableName> tables = sut.listTables(session, Optional.of("casens"));
        Set<String> expected = Set.of("casenstable");
        assertEquals(tables.size(), expected.size());
        assertEquals(tables.stream().map(SchemaTableName::getTableName).collect(Collectors.toSet()), expected);
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
    public void testColumnsInSchema() {
        ConnectorTableHandle handle = sut.getTableHandle(session, new SchemaTableName("casens", "casenstable"));
        Map<String, ColumnHandle> columns = sut.getColumnHandles(session, handle);
        assertEquals(columns.size(), 2);
    }

    @Test
    public void testColumnsCachingAcrossSchemas() {
        ConnectorTableHandle handle = sut.getTableHandle(session, new SchemaTableName("default", "ctable"));
        Map<String, ColumnHandle> columns = sut.getColumnHandles(session, handle);
        assertEquals(columns.size(), 4);

        handle = sut.getTableHandle(session, new SchemaTableName("myns", "ctable"));
        columns = sut.getColumnHandles(session, handle);
        assertEquals(columns.size(), 2);
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
    public void testTableWithEmptyType() {
        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "([] a: 1 2 3; b: (`a; 1; 2021.01.01))"));
        Map<String, KDBColumnHandle> columns = (Map) sut.getColumnHandles(session, handle);

        assertEquals(columns.size(), 2);
        assertEquals(columns.values().stream().map(c -> c.getKdbType()).collect(Collectors.toSet()), Set.of(KDBType.Long, KDBType.Unknown));
    }

    @Test
    public void testTableStats() throws Exception {
        Config cfg = new Config(Map.of(Config.KDB_USE_STATS_KEY, "false"));
        ConnectorSession session = TestingConnectorSession.builder().setPropertyMetadata(cfg.getSessionProperties()).build();
        KDBClientFactory factory = new KDBClientFactory("localhost", 8000, "user", "password", Optional.empty(), Optional.empty());
        KDBMetadata metadata = new KDBMetadata(factory, cfg, new StatsManager(factory));
        TableStatistics stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")));
        assertEquals(stats, TableStatistics.empty());

        // stats = true, dynanic stats = false
        cfg = new Config(Map.of(Config.KDB_USE_STATS_KEY, "true"));
        session = TestingConnectorSession.builder().setPropertyMetadata(cfg.getSessionProperties()).build();
        metadata = new KDBMetadata(factory, cfg, new StatsManager(factory));
        stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")));
        assertEquals(stats, TableStatistics.empty());

        cfg = new Config(Map.of(Config.KDB_USE_STATS_KEY, "true", Config.KDB_DYNAMIC_STATS, "true"));
        session = TestingConnectorSession.builder().setPropertyMetadata(cfg.getSessionProperties()).build();
        metadata = new KDBMetadata(factory, cfg, new StatsManager(factory));
        stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")));
        assertEquals(stats.getRowCount().getValue(), 3.0, 0.1);
    }

    @Test
    public void testPreGeneratedTableStats() throws Exception {
        Config cfg = new Config(Map.of(Config.KDB_USE_STATS_KEY, "true", Config.KDB_DYNAMIC_STATS, "false"));
        ConnectorSession session = TestingConnectorSession.builder().setPropertyMetadata(cfg.getSessionProperties()).build();
        kx.c connection = new kx.c("localhost", 8000, "user:password");
        connection.k(".trino.stats:([table:`atable`btable] rowcount:10000 20000)");
        connection.k(".trino.colstats:([table:`atable`atable; column:`name`iq] distinct_count:10000 5000; null_fraction: 0.0 0.0; size: 40000 80000; min_value: 0n 50.0; max_value: 0n 300.0)");

        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "atable"));
        TableStatistics stats = sut.getTableStatistics(session, handle);

        assertEquals(stats.getRowCount().getValue(), 10000.0, 0.1);
        Map<String, ColumnStatistics> colStats = stats.getColumnStatistics().entrySet().stream().collect(Collectors.toMap(e -> ((KDBColumnHandle) e.getKey()).getName(), e -> e.getValue()));

        ColumnStatistics cstats = colStats.get("name");
        assertEquals(cstats.getNullsFraction().getValue(), 0.0, 0.01);
        assertEquals(cstats.getDistinctValuesCount().getValue(), 10000.0, 0.01);
        assertEquals(cstats.getDataSize().getValue(), 40000.0, 0.1);
        assertTrue(cstats.getRange().isEmpty());

        cstats = colStats.get("iq");
        assertEquals(cstats.getNullsFraction().getValue(), 0.0, 0.01);
        assertEquals(cstats.getDistinctValuesCount().getValue(), 5000.0, 0.01);
        assertEquals(cstats.getDataSize().getValue(), 80000.0, 0.1);
        assertEquals(cstats.getRange().get().getMin(), 50.0, 0.1);
        assertEquals(cstats.getRange().get().getMax(), 300.0, 0.1);
    }

    @Test
    public void testPreGeneratedTableStatsDontExist() throws Exception {
        kx.c connection = new kx.c("localhost", 8000, "user:password");
        connection.k(".trino.stats:([table:`atable`btable] rowcount:10000 20000)");

        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "ctable"));
        TableStatistics stats = sut.getTableStatistics(session, handle);
        assertEquals(stats.getRowCount().getValue(), 1000000.0, 0.1);
    }

    @Test
    public void testPartitionedTableStats() {
        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "partition_table"));
        TableStatistics stats = sut.getTableStatistics(session, handle);
        assertEquals(stats.getRowCount().getValue(), 12.0, 0.1);
    }

    @Test
    public void testNoStatsForPassThroughQuery() {
        KDBTableHandle handle = (KDBTableHandle) sut.getTableHandle(session, new SchemaTableName("default", "([] a:1 2 3)"));
        TableStatistics stats = sut.getTableStatistics(session, handle);
        assertTrue(stats.getRowCount().isUnknown());
    }
}

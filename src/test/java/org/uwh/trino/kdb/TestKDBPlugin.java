package org.uwh.trino.kdb;

import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.testing.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.testng.Assert.*;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestKDBPlugin extends AbstractTestQueryFramework {

    public static void initKDB(kx.c conn) throws Exception {
        // create test tables
        conn.k("atable:([] name:`Dent`Beeblebrox`Prefect; iq:98 42 126)");
        conn.k("btable:([] booleans:001b; guids: 3?0Ng; bytes: `byte$1 2 3; shorts: `short$1 2 3; ints: `int$1 2 3; longs: `long$1 2 3; reals: `real$1 2 3; floats: `float$1 2 3; chars:\"abc\"; strings:(\"hello\"; \"world\"; \"trino\"); symbols:`a`b`c; timestamps: `timestamp$1 2 3; months: `month$1 2 3; dates: `date$1 2 3; datetimes: `datetime$1 2 3; timespans: `timespan$1 2 3; minutes: `minute$1 2 3; seconds: `second$1 2 3; times: `time$1 2 3 )");
        conn.k("ctable:([] const:1000000#1; linear:til 1000000; sym:1000000#`hello`world`trino; s:1000000#string `hello`world`trino)");
        conn.k("dtable:([] num:1 2 3; num_array: (1 2 3; 3 4 5; 6 7 8))");
        conn.k("keyed_table:([name:`Dent`Beeblebrox`Prefect] iq:98 42 126)");
        conn.k("attribute_table:([] unique_col: `u#`a`b`c; sorted_col: `s#1 2 3; parted_col: `p#1 1 2; grouped_col: `g#`a`b`c; plain_col: 1 2 3)");
        conn.k("CaseSensitiveTable:([] Number: 1 2 3 4; Square: 1 4 9 16)");

        conn.k("tfunc:{[] atable}");
        Path tempp = Files.createTempDirectory("splay");
        Path p = tempp.resolve("splay_table");
        String dirPath = p.toAbsolutePath().toString();
        conn.k("`:" + dirPath + "/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("\\l "+dirPath);

        Path p2 = tempp.resolve("db");
        dirPath = p2.toAbsolutePath().toString();
        Files.createDirectories(p2);
        System.out.println(dirPath);
        conn.k("`:"+dirPath + "/2021.05.28/partition_table/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("`:"+dirPath + "/2021.05.29/partition_table/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("`:"+dirPath + "/2021.05.30/partition_table/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("`:"+dirPath + "/2021.05.31/partition_table/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("\\l " + dirPath);
    }

    @BeforeClass
    public static void setupKDB() throws Exception {
        kx.c conn = new kx.c("localhost", 8000, "user:password");
        initKDB(conn);

        Logger.getLogger(KDBClient.class.getName()).addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String msg = record.getMessage();
                if (msg.startsWith("KDB query: ")) {
                    lastQuery = msg.substring("KDB query: ".length());
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() throws SecurityException {}
        });
    }

    // this test kills the local KDB instance and is a bit of a pain to run, hence disabled by default
    @Test(enabled = false)
    public void testReconnect() throws Exception {
        kx.c conn = new kx.c("localhost", 8000, "user:password");
        String command = new String((char[]) conn.k("(system \"pwd\")[0] , \"/\" , \" \" sv .z.X"));
        conn.ks("exit 0");

        Runtime.getRuntime().exec(command);
        Thread.sleep(50);

        conn = new kx.c("localhost", 8000, "user:password");
        initKDB(conn);

        query("select * from atable", 3);
    }

    @Test
    public void testKDBDownDoesNotStopCatalogCreation() throws Exception {
        DistributedQueryRunner qrunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(1)
                .setExtraProperties(ImmutableMap.of())
                .build();

        qrunner.installPlugin(new KDBPlugin());
        qrunner.createCatalog("kdb", "kdb",
                ImmutableMap.<String,String>builder()
                        .put("kdb.host", "localhost")
                        .put("kdb.port", "12345")
                        .build());

        // only throw exception once operation is invoked
        try {
            qrunner.execute(createSession("default"), "show tables");
            fail();
        } catch (Exception e) {
            LOGGER.info(e.toString());
        }
    }

    @Test
    public void testQuery() {
        query("select * from atable", 3);
        assertLastQuery("select [50000] from select name, iq from atable");
        assertResultColumn(0, Set.of("Dent", "Beeblebrox", "Prefect"));
    }

    @Test
    public void testPagination() {
        query("select linear from ctable limit 120000", 120000);
        assertLastQuery("select [100000 20000] from select linear from ctable where i<120000");
    }

    @Test
    public void testLargeCountQuery() {
        query("select count(*) from ctable", 1);
        assertEquals(res.getOnlyColumnAsSet(), Set.of(1_000_000L));

        query("select sum(linear) from ctable", 1);
        assertEquals(res.getOnlyColumnAsSet(), Set.of(499999500000L));
    }

    @Test
    public void testPassThroughQuery() {
        query("select * from kdb.default.\"select max iq from atable\"", 1);
        assertLastQuery("select [50000] from select iq from (select max iq from atable)");
        assertEquals(res.getOnlyColumnAsSet(), Set.of(126L));
    }

    @Test
    public void testSplayTableQuery() {
        query("select * from splay_table", 3);
        assertLastQuery("select [50000] from select v1, v2 from splay_table");
        assertResultColumn(0, Set.of(10L, 20L, 30L));
    }

    @Test
    public void testTableWithUnknownType() {
        query("select * from \"([] a: 1 2 3; b: (`a; 1; 2021.01.01))\"", 3);
        assertResultColumn(1, Set.of("a", "1", "2021-01-01"));
    }


    @Test
    public void testFilterPushdown() {
        query("select * from atable where iq > 50", 2);
        assertLastQuery("select [50000] from select name, iq from atable where iq > 50");
        assertResultColumn(0, Set.of("Dent", "Prefect"));
    }

    @Test
    public void testFilterPushdownMultiple() {
        query("select * from atable where iq > 50 and iq < 100", 1);
        assertLastQuery("select [50000] from select name, iq from atable where (iq > 50) & (iq < 100)");
        assertResultColumn(0, Set.of("Dent"));
    }

    @Test
    public void testFilterPushdownSymbol() {
        query("select * from atable where name = 'Dent'", 1);
        assertLastQuery("select [50000] from select name, iq from atable where name = `Dent");
        assertResultColumn(0, Set.of("Dent"));
    }

    @Test
    public void testFilterWithAttributes() {
        query("select * from \"([] id:`s#0 1 2; name:`alice`bob`charlie; age:28 35 28)\" where age = 28 and id = 0", 1);
        assertLastQuery("select [50000] from select id, name, age from (([] id:`s#0 1 2; name:`alice`bob`charlie; age:28 35 28)) where id = 0, age = 28");
    }

    @Test
    public void testAggregationPushdown() {
        // count(*)
        query("select count(*) from atable", 1);
        assertResultColumn(0, Set.of(3L));
        assertLastQuery("select [50000] from select col0 from (select col0: count i from atable)");

        // sum(_)
        query("select sum(iq) from atable", 1);
        assertResultColumn(0, Set.of(98L+42L+126L));
        assertLastQuery("select [50000] from select col0 from (select col0: sum iq from atable)");

        // sum(_) and count(*)
        query("select sum(iq), count(*) from atable", 1);
        assertResultColumn(0, Set.of(98L+42L+126L));
        assertResultColumn(1, Set.of(3L));
        assertLastQuery("select [50000] from select col0, col1 from (select col0: sum iq, col1: count i from atable)");

        // count(distinct)
        query("select count(distinct sym) from \"([] sym: `a`a`b)\"", 1);
        assertResultColumn(0, Set.of(2L));
        assertLastQuery("select [50000] from select col0 from (select col0: count sym from select count i by sym from ([] sym: `a`a`b))");

        // count(distinct) #2
        query("select sym, count(distinct sym2) from \"([] sym: `a`a`b`b; sym2: `a`b`c`c)\" group by sym", 2);
        assertResultColumn(1, Set.of(1L, 2L));
        assertLastQuery("select [50000] from select sym, col0 from (select col0: count sym2 by sym from select count i by sym, sym2 from ([] sym: `a`a`b`b; sym2: `a`b`c`c))");

        // sum group by
        query("select sym, sum(num) from \"([] sym: `a`a`b; num: 2 3 4)\" group by sym", 2);
        assertResultColumn(0, Set.of("a","b"));
        assertResultColumn(1, Set.of(5L, 4L));
        assertLastQuery("select [50000] from select sym, col0 from (select col0: sum num by sym from ([] sym: `a`a`b; num: 2 3 4))");

        // sum group by multiple
        query("select sym, sym2, sum(num) from \"([] sym: `a`a`b`b; sym2: `a`a`b`c; num: 2 3 4 6)\" group by sym, sym2", 3);
        assertResultColumn(2, Set.of(5L, 4L, 6L));
        assertLastQuery("select [50000] from select sym, sym2, col0 from (select col0: sum num by sym, sym2 from ([] sym: `a`a`b`b; sym2: `a`a`b`c; num: 2 3 4 6))");

        // aggregation with filter
        query("select sym, sum(num) from \"([] sym: `a`a`b`b; sym2: `a`b`a`b; num: 2 3 4 5)\" where sym2 = 'a' group by sym", 2);
        assertResultColumn(0, Set.of("a", "b"));
        assertResultColumn(1, Set.of(2L, 4L));

        // aggregation with nested limit
        query("select sym, sum(num) from (select * from \"([] sym: `a`a`a; num: 2 3 4)\" limit 2) t group by sym", 1);
        assertResultColumn(1, Set.of(5L));
        // currently due to the limit statement not being marked as 100% reliable aggregation is not pushed down to KDB
        assertLastQuery("select [2] from select sym, num from (([] sym: `a`a`a; num: 2 3 4)) where i<2");
    }

    @Test
    public void testSessionPushDownAggregationOverride() {
        Session session = Session.builder(getSession()).setCatalogSessionProperty("kdb", "push_down_aggregation", "false").build();
        query(session, "select count(*) from atable");
        assertLastQuery("select [50000] from select i from atable");
    }

    @Test
    public void testDescribe() {
        query("describe atable", 2);
        assertResultColumn(0, Set.of("name","iq"));
    }

    @Test
    public void testTypeSupport() {
        query("select * from btable", 3);

        query("select strings, symbols from btable where strings = 'hello'", 1);
        assertLastQuery("select [50000] from select strings, symbols from btable where strings like \"hello\"");

        query("select strings, symbols from btable where strings = 'h'", 0);
        assertLastQuery("select [50000] from select strings, symbols from btable where strings like (enlist \"h\")");

        query("select dates, symbols from btable where dates = DATE '2000-01-02'", 1);
        assertLastQuery("select [50000] from select symbols, dates from btable where dates = 2000.01.02");

        query("select dates, symbols from btable where dates BETWEEN DATE '2000-01-01' AND DATE '2000-01-03'", 2);
        assertLastQuery("select [50000] from select symbols, dates from btable where dates within 2000.01.01 2000.01.03");
    }

    @Test
    public void testNullHandling() {
        query("select * from \"([] ds:(2021.05.30; 0nd; 2021.05.31))\"",3);
        List<MaterializedRow> rows = res.getMaterializedRows();
        assertEquals(rows.get(0).getField(0), LocalDate.of(2021,5,30));
        assertNull(rows.get(1).getField(0));
        assertEquals(rows.get(2).getField(0), LocalDate.of(2021,5,31));

        query("select * from \"([] " +
                        "type_g:(0Ng; 0x0 sv 16?0xff); " +
                        "type_h: (0Nh; 1h); " +
                        "type_i: (0Ni; 1i); " +
                        "type_j: (0Nj; 1j); " +
                        "type_e: (0Ne; 1.0e); " +
                        "type_f: (0Nf; 1.0); " +
                        "type_s: ``abc; " +
                        "type_p: (0Np; `timestamp$1); " +
                        "type_m: (0Nm; 2020.01m); " +
                        "type_d: (0Nd; 2020.01.01); " +
                        "type_z: (0Nz; .z.z); " +
                        "type_n: (0Nn; `timespan$1); " +
                        "type_u: (0Nu; `minute$1); " +
                        "type_v: (0Nv; `second$1); " +
                        "type_t: (0Nt; `time$1)" +
                        ")\"",
                2);
        rows = res.getMaterializedRows();
        for (int i=0; i<rows.get(0).getFieldCount(); i++) {
            assertNull(rows.get(0).getField(i));
            assertNotNull(rows.get(1).getField(i));
        }

        query("select * from \"([] dates:(2021.05.31 0Nd; 2021.01.01 2021.01.02))\"", 2);
        List list = (List) res.getMaterializedRows().get(0).getField(0);
        assertNull(list.get(1));
    }

    @Test
    public void testTimeTypes() {
        query("select * from \"([] t: (23:55:00.000; 23:59:59.000))\"",2);
        assertResultColumn(0, Set.of(LocalTime.of(23,55,00), LocalTime.of(23,59,59)));

        query("select * from \"([] t: (2021.05.31\\T23:55:00; 2021.06.01\\T01:00:00))\"", 2);
        assertResultColumn(0, Set.of(
                LocalDateTime.of(2021,5,31, 23,55,00),
                LocalDateTime.of(2021, 6,1, 1,0,0)));

        query("select * from \"([] t: (2021.05.31\\D23:55:00; 2021.06.01\\D01:00:00))\"", 2);
        assertResultColumn(0, Set.of(
                LocalDateTime.of(2021,5,31, 23,55,00),
                LocalDateTime.of(2021, 6,1, 1,0,0)));
    }


    @Test
    public void testFilterOrdering() {
        query("select count(*) from ctable where s = 'trino' and sym = 'trino'", 1);
        assertLastQuery("select [50000] from select col0 from (select col0: count i from ctable where sym = `trino, s like \"trino\")");
    }

    @Test
    public void testKeyedTableQuery() {
        query("select * from keyed_table", 3);

        // pass through query
        query("select * from \"select from keyed_table\"", 3);
    }

    @Test
    public void testStoredProc() {
        query("select * from \"tfunc[]\"", 3);
    }

    @Test
    public void testLimit() {
        query("select * from atable limit 2", 2);
        assertLastQuery("select [2] from select name, iq from atable where i<2");

        query("select const, linear from ctable where const = 1 limit 10", 10);
        assertLastQuery("select [10] from select const, linear from ctable where const = 1");

        query("select * from \"select from atable\" limit 2", 2);
        assertLastQuery("select [2] from select name, iq from (select from atable) where i<2");

        // test limit greater than matching rows
        query("select name from atable where iq < 100 limit 10", 2);
        assertResultColumn(0, Set.of("Dent", "Beeblebrox"));
        assertLastQuery("select [10] from select name from atable where iq < 100");
    }

    @Test
    public void testNestedArray() {
        // array of long
        query("select * from dtable", 3);
        assertEquals(res.getTypes(), List.of(BigintType.BIGINT, new ArrayType(BigintType.BIGINT)));

        // array of double
        query("select * from \"([] col:(1.0 2.0; 3.0 4.0))\"", 2);
        // array of symbols
        query("select * from \"([] col:(`a`b`c; `d`e`f))\"", 2);
    }

    @Test
    public void testPartitionedTable() {
        query("select count(*) from partition_table", 1);
        // 3 rows * 4 days
        assertEquals(res.getOnlyColumnAsSet(), Set.of(12L));

        // 3 rows in one date
        query("select count(*) from partition_table where date = DATE '2021-05-28'", 1);
        assertEquals(res.getOnlyColumnAsSet(), Set.of(3L));
    }

    @Test
    public void testPartitionedTableQuerySplit() {
        query("select * from partition_table", 12);
        assertTrue(Set.of(
                "select [50000] from select date, v1, v2 from partition_table where date = 2021.05.28",
                "select [50000] from select date, v1, v2 from partition_table where date = 2021.05.29",
                "select [50000] from select date, v1, v2 from partition_table where date = 2021.05.30",
                "select [50000] from select date, v1, v2 from partition_table where date = 2021.05.31"
        ).contains(lastQuery));
    }

    @Test
    public void testCaseSensitiveTable() {
        query("select * from CaseSensitiveTable", 4);
    }

    // no solution yet for dynamic queries
    @Test
    public void testCaseSensitiveQuery() {
        query("select * from \"select from \\Case\\Sensitive\\Table\"", 4);
    }

    private static String lastQuery = null;
    private MaterializedResult res;
    private static Logger LOGGER = Logger.getLogger(TestKDBPlugin.class.getName());

    private void query(String sql, int expected) {
        res = computeActual(sql);
        if (res.getRowCount() < 100) {
            LOGGER.info("Query results: " + res);
        } else {
            LOGGER.info("Query results: " + res.getRowCount() + " rows");
        }
        assertEquals(res.getRowCount(), expected);
    }

    private void assertLastQuery(String ksql) {
        assertEquals(lastQuery, ksql);
    }

    private void assertResultColumn(int idx, Set expected) {
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(idx)).collect(Collectors.toSet()), expected);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        DistributedQueryRunner qrunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(1)
                .setExtraProperties(ImmutableMap.of())
                .build();

        qrunner.installPlugin(new KDBPlugin());
        qrunner.createCatalog("kdb", "kdb",
                ImmutableMap.<String,String>builder()
                        .put("kdb.host", "localhost")
                        .put("kdb.port", "8000")
                        .put("kdb.user", "user")
                        .put("kdb.password", "password")
                        .build());

        return qrunner;
    }

    private static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("kdb")
                .setSchema(schema)
                .build();
    }

}

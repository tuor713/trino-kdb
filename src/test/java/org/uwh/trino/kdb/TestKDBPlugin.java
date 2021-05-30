package org.uwh.trino.kdb;

import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.testing.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
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

        conn.k("tfunc:{[] atable}");
        Path p = Files.createTempDirectory("splay");
        p = p.resolve("splay_table");
        String dirPath = p.toAbsolutePath().toString();
        conn.k("`:" + dirPath + " set ([] v1:10 20 30; v2:1.1 2.2 3.3)");
        conn.k("\\l "+dirPath);
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

    @Test
    public void testTableStats() throws Exception {
        ConnectorSession session = TestingConnectorSession.builder().build();
        KDBMetadata metadata = new KDBMetadata(new KDBClient("localhost", 8000, "user", "password"), false);
        TableStatistics stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")), Constraint.alwaysTrue());
        assertEquals(stats, TableStatistics.empty());

        metadata = new KDBMetadata(new KDBClient("localhost", 8000, "user", "password"), true);
        stats = metadata.getTableStatistics(session, metadata.getTableHandle(session, new SchemaTableName("default", "atable")), Constraint.alwaysTrue());
        assertEquals(stats.getRowCount().getValue(), 3.0, 0.1);
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
    public void testQuery() {
        query("select * from atable", 3);
        assertLastQuery("select [50000] from select name, iq from atable");
        assertResultColumn(0, Set.of("Dent", "Beeblebrox", "Prefect"));
    }

    @Test
    public void testLargeCountQuery() {
        query("select count(*) from ctable", 1);
        assertLastQuery("select [1000000 50000] from select i from ctable");
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
    public void testFilterOrdering() {
        query("select count(*) from ctable where s = 'trino' and sym = 'trino'", 1);
        assertLastQuery("select [300000 50000] from select sym, s from ctable where sym = `trino, s like \"trino\"");
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
        assertLastQuery("select [50000] from select name, iq from atable where i<2");

        query("select const, linear from ctable where const = 1 limit 10", 10);
        // Optimizer does not appear to attempt to push limit into filter, instead filter is pushed and limit post-applied
        assertLastQuery("select [50000] from select const, linear from ctable where const = 1");

        query("select * from \"select from atable\" limit 2", 2);
        assertLastQuery("select [50000] from select name, iq from (select from atable) where i<2");
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

    private static String lastQuery = null;
    private MaterializedResult res;
    private static Logger LOGGER = Logger.getLogger(TestKDBPlugin.class.getName());

    private void query(String sql, int expected) {
        res = computeActual(sql);
        LOGGER.info("Query results: " + res);
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

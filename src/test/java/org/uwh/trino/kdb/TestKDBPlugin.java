package org.uwh.trino.kdb;

import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.*;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test
public class TestKDBPlugin extends AbstractTestQueryFramework {
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

    /*
    KDB setup:
    atable:([] name:`Dent`Beeblebrox`Prefect; iq:98 42 126)
    btable:([] booleans:001b; guids: 3?0Ng; bytes: `byte$1 2 3; shorts: `short$1 2 3; ints: `int$1 2 3; longs: `long$1 2 3; reals: `real$1 2 3; floats: `float$1 2 3; chars:"abc"; strings:("hello"; "world"; "trino"); symbols:`a`b`c; timestamps: `timestamp$1 2 3; months: `month$1 2 3; dates: `date$1 2 3; datetimes: `datetime$1 2 3; timespans: `timespan$1 2 3; minutes: `minute$1 2 3; seconds: `second$1 2 3; times: `time$1 2 3 )
    ctable:([] const:1000000#1; linear:til 1000000)
     */

    @Test
    public void testMetadata() throws Exception {
        ConnectorSession session = TestingConnectorSession.builder().build();
        KDBMetadata metadata = new KDBMetadata(new KDBClient("localhost", 8000, "user", "password"));
        List<SchemaTableName> tables = metadata.listTables(session, Optional.empty());
        assertEquals(3, tables.size());
        assertEquals(Set.of("atable","btable","ctable"), tables.stream().map(t -> t.getTableName()).collect(Collectors.toSet()));
    }

    @Test
    public void testQuery() {
        MaterializedResult res = computeActual("select * from atable");
        System.out.println(res);
        assertEquals(res.getRowCount(), 3);
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(0)).collect(Collectors.toSet()), Set.of("Dent", "Beeblebrox", "Prefect"));
    }

    @Test
    public void testLargeCountQuery() {
        MaterializedResult res = computeActual("select count(*) from ctable");
        System.out.println(res);
        assertEquals(res.getRowCount(), 1);
        assertEquals(res.getOnlyColumnAsSet(), Set.of(1_000_000L));
    }

    @Test
    public void testPassThroughQuery() {
        MaterializedResult res = computeActual("select * from kdb.default.\"select max iq from atable\"");
        System.out.println(res);
        assertEquals(res.getRowCount(), 1);
        assertEquals(res.getOnlyColumnAsSet(), Set.of(126L));
    }

    @Test
    public void testFilterPushdown() {
        MaterializedResult res = computeActual("select * from atable where iq > 50");
        System.out.println(res);
        assertEquals(res.getRowCount(), 2);
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(0)).collect(Collectors.toSet()), Set.of("Dent", "Prefect"));
    }

    @Test
    public void testFilterPushdownMultiple() {
        MaterializedResult res = computeActual("select * from atable where iq > 50 and iq < 100");
        System.out.println(res);
        assertEquals(res.getRowCount(), 1);
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(0)).collect(Collectors.toSet()), Set.of("Dent"));
    }

    @Test
    public void testFilterPushdownSymbol() {
        MaterializedResult res = computeActual("select * from atable where name = 'Dent'");
        System.out.println(res);
        assertEquals(res.getRowCount(), 1);
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(0)).collect(Collectors.toSet()), Set.of("Dent"));
    }

    @Test
    public void testDescribe() {
        MaterializedResult res = computeActual("describe atable");
        System.out.println(res);
        assertEquals(res.getRowCount(), 2);
        assertEquals(res.getMaterializedRows().stream().map(row -> row.getField(0)).collect(Collectors.toSet()), Set.of("name","iq"));
    }

    @Test
    public void testTypeSupport() {
        MaterializedResult res = computeActual("select * from btable");
        System.out.println(res);
        assertEquals(res.getRowCount(), 3);
    }

}

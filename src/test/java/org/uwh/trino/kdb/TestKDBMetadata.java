package org.uwh.trino.kdb;

import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
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
        sut = new KDBMetadata(new KDBClient("localhost", 8000, "user", "password"), false);
    }

    @Test
    public void testListTables() {
        List<SchemaTableName> tables = sut.listTables(session, Optional.empty());

        Set<String> expected = Set.of("atable", "btable", "ctable", "dtable", "keyed_table", "splay_table", "attribute_table", "partition_table");

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
}

package org.uwh.trino.kdb;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import kx.c;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KDBClient {
    private static final Logger LOGGER = Logger.get(KDBClient.class);
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private kx.c connection = null;

    public KDBClient(String host, int port, String user, String password) throws Exception {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private void connect() throws Exception {
        if (user != null && password != null) {
            connection = new c(host, port, user + ":" + password);
        } else {
            connection = new c(host, port);
        }
    }

    private Object exec(String expr) throws Exception {
        if (connection == null) {
            connect();
        }

        try {
            LOGGER.info("KDB query: "+expr);
            return connection.k(expr);
        // SocketException & EOFEXception
        } catch (IOException e) {
            // happens when connection has been lost, for example KDB process restarted
            // try reconnect
            connect();
            return connection.k(expr);
        }
    }

    private Object exec(String expr, Object arg) throws Exception {
        if (connection == null) {
            connect();
        }

        try {
            LOGGER.info("KDB query: "+expr);
            return connection.k(expr, arg);
            // SocketException & EOFEXception
        } catch (IOException e) {
            // happens when connection has been lost, for example KDB process restarted
            // try reconnect
            connect();
            return connection.k(expr, arg);
        }
    }

    public List<String> listNamespaces() throws Exception {
        String[] res = (String[]) exec("exec distinct ns from (uj/) ({[ns] ns:`$\".\", string ns; ts: tables ns; ([] ns:(count ts)#ns; table:ts)} each ((enlist `) , key `))");
        return Arrays.stream(res).map(ns -> ns.substring(1)).collect(Collectors.toList());
    }

    public List<String> listTables(String ns) throws Exception {
        String[] res = (String[]) exec("tables `."+ns);
        return Arrays.asList(res);
    }

    public List<String[]> listTables() throws Exception {
        c.Flip res = (c.Flip) exec("(uj/) ({[ns] ns:`$\".\", string ns; ts: tables ns; ([] ns:(count ts)#ns; table:ts)} each ((enlist `) , key `))");
        String[] namespaces = (String[]) res.y[0];
        String[] tables = (String[]) res.y[1];
        return IntStream.range(0, namespaces.length).mapToObj(i -> new String[] { namespaces[i].substring(1), tables[i]}).collect(Collectors.toList());
    }

    private boolean isPartitioned(String name) throws Exception {
        return !KDBTableHandle.isQuery(name) && (boolean) exec("`boolean$.Q.qp["+name+"]");
    }

    public KDBTableHandle getTableHandle(String namespace, String name) throws Exception {
        String qualifiedTableName = (namespace.equals("") ? "" : "." + namespace + ".") + name;
        boolean isPartitioned = isPartitioned(qualifiedTableName);
        List<String> partitions = List.of();
        Optional<KDBColumnHandle> partitionColumn = Optional.empty();
        if (isPartitioned) {
            Object[] colInfo = (Object[]) exec("((0!meta " + qualifiedTableName + ")[`c][0]; (0!meta " + qualifiedTableName + ")[`t][0])");
            String colName = (String) colInfo[0];
            KDBType colType = KDBType.fromTypeCode((char) colInfo[1]);
            partitionColumn = Optional.of(new KDBColumnHandle(colName, colType.getTrinoType(), colType, Optional.empty(), true));

            partitions = new ArrayList<>();
            for (Object partition : (Object[]) exec("string (select distinct " + colName +" from " + qualifiedTableName + ")[`" + colName + "]")) {
                partitions.add(new String((char[]) partition));
            }
        }

        return new KDBTableHandle(namespace, name, TupleDomain.all(), OptionalLong.empty(), isPartitioned, partitionColumn, partitions);
    }

    public List<ColumnMetadata> getTableMeta(KDBTableHandle handle) throws Exception {
        boolean isPartitioned = handle.isPartitioned();

        c.Dict res = (c.Dict) exec("meta "+handle.getQualifiedTableName());
        c.Flip columns = (c.Flip) res.x;
        c.Flip colMeta = (c.Flip) res.y;
        String[] colNames = (String[]) columns.y[0];
        char[] types = (char[]) colMeta.y[0];
        String[] attributes = (String[]) colMeta.y[2];

        List<ColumnMetadata> result = new ArrayList<>();
        for (int i=0; i<colNames.length; i++) {
            KDBType kdbType = KDBType.fromTypeCode(types[i]);
            Map<String,Object> props = Map.of(
                    // need to capture this because ColumnMetadata lower cases the column
                    "kdb.name", colNames[i],
                    "kdb.type", kdbType,
                    "kdb.attribute", attributes[i] != null && !attributes[i].isEmpty() ? Optional.of(KDBAttribute.fromCode(attributes[i].charAt(0))) : Optional.empty(),
                    "kdb.isPartitionColumn", isPartitioned && i == 0
            );

            ColumnMetadata col = new ColumnMetadata(
                    colNames[i],
                    kdbType.getTrinoType(),
                    null,
                    null,
                    false,
                    props);
            result.add(col);
        }

        return ImmutableList.copyOf(result);
    }

    public Page getData(KDBTableHandle handle, List<KDBColumnHandle> columns, int page, int pageSize, boolean isVirtualTables) throws Exception {
        // "select count(*) type use cases
        if (columns.isEmpty()) {
            columns = List.of(new KDBColumnHandle("i", BigintType.BIGINT, KDBType.Long, null, false));
            // one more weird exception select date from <partitioned table> where date = <x> gives only a single row
        } else if (columns.size() == 1 && columns.get(0).isPartitionColumn()) {
            columns = List.of(columns.get(0), new KDBColumnHandle("i", BigintType.BIGINT, KDBType.Long, null, false));
        }

        c.Flip res = (c.Flip) exec(handle.toQuery(columns, OptionalInt.of(page), pageSize, isVirtualTables));

        PageBuilder builder = new PageBuilder(columns.stream().map(col -> col.getType()).collect(Collectors.toList()));

        builder.declarePositions(getArrayLength(columns.get(0).getType(), res.y[0]));
        for (int i=0; i<columns.size(); i++) {
            BlockBuilder bb = builder.getBlockBuilder(i);
            columns.get(i).getKdbType().writeBlock(bb, res.y[i]);
        }

        return builder.build();
    }

    public void writeData(String table, List<KDBColumnHandle> columns, Page page, String insertFunction) throws Exception {
        c.Dict dict = new c.Dict(
                columns.stream().map(KDBColumnHandle::getName).toArray(String[]::new),
                IntStream.range(0, page.getChannelCount()).mapToObj(idx -> {
                    KDBColumnHandle col = columns.get(idx);
                    return col.getKdbType().readBlock(page.getBlock(idx));
                }).toArray());
        c.Flip flip = new c.Flip(dict);

        exec(insertFunction + "[`"+table+";]", flip);
    }

    private Optional<TableStatistics> getPregeneratedStats(KDBTableHandle table) throws Exception {
        boolean hasStats = (boolean) exec("`stats in key `.trino");
        if (!hasStats) {
            return Optional.empty();
        }

        long[] rows = (long[]) exec("exec rowcount from .trino.stats where table = `" + table.getQualifiedTableName());
        if (rows.length == 0) {
            // No pre-generated stats for this table
            return Optional.empty();
        }

        TableStatistics.Builder builder = TableStatistics.builder().setRowCount(Estimate.of(rows[0]));

        Map<String,ColumnMetadata> colMap = getTableMeta(table).stream().collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
        c.Flip colMeta = (c.Flip) exec("select column, distinct_count, null_fraction, size, min_value, max_value from .trino.colstats where table = `" + table.getQualifiedTableName());
        String[] columns = (String[]) colMeta.y[0];
        long[] distinctCounts = (long[]) colMeta.y[1];
        double[] nullFractions = (double[]) colMeta.y[2];
        long[] sizes = (long[]) colMeta.y[3];
        double[] mins = (double[]) colMeta.y[4];
        double[] maxs = (double[]) colMeta.y[5];

        for (int i=0; i<columns.length; i++) {
            ColumnMetadata meta = colMap.get(columns[i]);
            if (meta != null) {
                builder.setColumnStatistics(
                        columnMetaToHandle(meta),
                        new ColumnStatistics(
                                Estimate.of(nullFractions[i]),
                                Estimate.of(distinctCounts[i]),
                                Estimate.of(sizes[i]),
                                (Double.isNaN(mins[i]) || Double.isNaN(maxs[i]))
                                        ? Optional.empty()
                                        : Optional.of(new DoubleRange(mins[i], maxs[i]))
                        )
                );
            }
        }

        return Optional.of(builder.build());
    }

    public TableStatistics getTableStatistics(KDBTableHandle table) throws Exception {
        LOGGER.info("Collecting statistics for table " + table.getQualifiedTableName());

        Optional<TableStatistics> preGeneratedStats = getPregeneratedStats(table);
        if (preGeneratedStats.isPresent()) {
            return preGeneratedStats.get();
        }

        long rows = (long) exec("count " + table.getQualifiedTableName());

        List<ColumnMetadata> columnMetadata = getTableMeta(table);
        String colQuery;
        if (table.isPartitioned()) {
            KDBColumnHandle parCol = table.getPartitionColumn().get();
            // -22 does not work on the whole table, so calculate partition by partition
            colQuery = columnMetadata.stream()
                    .map(col -> "(select name:`" + col.getName() + ", dcount, ncount, size from " +
                            "update size:(+/) {[v] (select count i, size:-22!" + col.getName() + " from "+ table.getQualifiedTableName() + " where " + parCol.getName() + " = v)[`size]} each (select distinct " + parCol.getName() +" from "+table.getQualifiedTableName()+")[`" + parCol.getName()+"] from "+
                            "select dcount: `long$(avg dcount) * (count " + table.getQualifiedTableName() + "), ncount: sum ncount from ((uj/) {[v] select dcount:(count distinct " + col.getName() + ") % (count i), " + ((col.getProperties().get("kdb.type") == KDBType.String) ? "ncount: sum `long$0 = count each " + col.getName(): "ncount: sum `long$null " + col.getName()) + " from " + table.getQualifiedTableName() + " where " + parCol.getName() + " = v} each (select distinct " + parCol.getName() + " from " + table.getQualifiedTableName() + ")[`" + parCol.getName() + "])"
                             + ")")
                    .collect(Collectors.joining(" uj "));
        } else {
            colQuery = columnMetadata.stream()
                    .map(col -> "(select name:`" + col.getName() + ", dcount, ncount, size " +
                            "from select dcount:count distinct " + col.getName() + ", " +
                            ((col.getProperties().get("kdb.type") == KDBType.String) ? "ncount: sum `long$0 = count each " + col.getName() + ", " : "ncount: sum `long$null " + col.getName() + ", ") +
                            "size: -22!" + col.getName() + " " +
                            "from " + table.getQualifiedTableName() + ")")
                    .collect(Collectors.joining(" uj "));
        }

        LOGGER.info("Column stats query: " + colQuery);

        c.Flip colMeta = (c.Flip) exec(colQuery);
        String[] columns = (String[]) colMeta.y[0];
        long[] distinctCounts = (long[]) colMeta.y[1];
        long[] nullCounts = (long[]) colMeta.y[2];
        long[] sizes = (long[]) colMeta.y[3];

        Map<ColumnHandle, ColumnStatistics> stats = new HashMap<>();
        for (int i=0; i<columns.length; i++) {
            stats.put(
                    columnMetaToHandle(columnMetadata.get(i)),
                    new ColumnStatistics(Estimate.of((double) nullCounts[i] / rows), Estimate.of(distinctCounts[i]), Estimate.of(sizes[i]), Optional.empty()));
        }

        return new TableStatistics(Estimate.of(rows), stats);
    }

    public static KDBColumnHandle columnMetaToHandle(ColumnMetadata meta) {
        return new KDBColumnHandle(
                meta.getName(),
                meta.getType(),
                (KDBType) meta.getProperties().get("kdb.type"),
                (Optional<KDBAttribute>) meta.getProperties().get("kdb.attribute"),
                (boolean) meta.getProperties().get("kdb.isPartitionColumn"));
    }

    private int getArrayLength(Type t, Object array) {
        if (array instanceof Object[]) {
            return ((Object[]) array).length;
        } else if (array instanceof long[]) {
            return ((long[]) array).length;
        } else if (array instanceof int[]) {
            return ((int[]) array).length;
        } else if (array instanceof double[]) {
            return ((double[]) array).length;
        } else if (array instanceof boolean[]) {
            return ((boolean[]) array).length;
        } else if (array instanceof float[]) {
            return ((float[]) array).length;
        } else if (array instanceof short[]) {
            return ((short[]) array).length;
        } else if (array instanceof byte[]) {
            return ((byte[]) array).length;
        } else if (array instanceof char[]) {
            return ((char[]) array).length;
        } else {
            throw new UnsupportedOperationException("Cannot get length of " + array);
        }
    }
}

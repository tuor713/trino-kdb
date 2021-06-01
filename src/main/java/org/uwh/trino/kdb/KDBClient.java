package org.uwh.trino.kdb;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import kx.c;

import java.io.EOFException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class KDBClient {
    private static final Logger LOGGER = Logger.get(KDBClient.class);
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private kx.c connection;

    public KDBClient(String host, int port, String user, String password) throws Exception {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        connect();
    }

    private void connect() throws Exception {
        if (user != null && password != null) {
            connection = new c(host, port, user + ":" + password);
        } else {
            connection = new c(host, port);
        }
    }

    private Object exec(String expr) throws Exception {
        try {
            return connection.k(expr);
        } catch (EOFException e) {
            // happens when connection has been lost, for example KDB process restarted
            // try reconnect
            connect();
            return connection.k(expr);
        }
    }

    public List<String> listTables() throws Exception {
        String[] res = (String[]) exec("system \"a\"");
        return Arrays.asList(res);
    }

    private boolean isPartitioned(String name) throws Exception {
        return !KDBTableHandle.isQuery(name) && (boolean) exec("`boolean$.Q.qp["+name+"]");
    }

    public KDBTableHandle getTableHandle(String schema, String name) throws Exception {
        boolean isPartitioned = isPartitioned(name);
        List<String> partitions = List.of();
        Optional<KDBColumnHandle> partitionColumn = Optional.empty();
        if (isPartitioned) {
            Object[] colInfo = (Object[]) exec("((0!meta " + name + ")[`c][0]; (0!meta " + name + ")[`t][0])");
            String colName = (String) colInfo[0];
            KDBType colType = KDBType.fromTypeCode((char) colInfo[1]);
            partitionColumn = Optional.of(new KDBColumnHandle(colName, colType.getTrinoType(), colType, Optional.empty(), true));

            partitions = new ArrayList<>();
            for (Object partition : (Object[]) exec("string (select distinct " + colName +" from " + name + ")[`" + colName + "]")) {
                partitions.add(new String((char[]) partition));
            }
        }

        return new KDBTableHandle(schema, name, TupleDomain.all(), OptionalLong.empty(), isPartitioned, partitionColumn, partitions);
    }

    public List<ColumnMetadata> getTableMeta(String name) throws Exception {
        boolean isPartitioned = isPartitioned(name);

        c.Dict res = (c.Dict) exec("meta "+name);
        c.Flip columns = (c.Flip) res.x;
        c.Flip colMeta = (c.Flip) res.y;
        String[] colNames = (String[]) columns.y[0];
        char[] types = (char[]) colMeta.y[0];
        String[] attributes = (String[]) colMeta.y[2];

        List<ColumnMetadata> result = new ArrayList<>();
        for (int i=0; i<colNames.length; i++) {
            KDBType kdbType = KDBType.fromTypeCode(types[i]);
            Map<String,Object> props = Map.of(
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

    private String formatKDBValue(KDBType type, Object value) {
        if (type == KDBType.String) {
            String s = ((Slice) value).toStringUtf8();
            if (s.length() < 2) {
                return "(enlist \"" + s + "\")";
            } else {
                return "\"" + s + "\"";
            }
        } else if (type == KDBType.Date) {
            LocalDate date = LocalDate.ofEpochDay((long) value);
            return date.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        } else if (value instanceof Slice) {
            Slice s = (Slice) value;
            return "`" + s.toStringUtf8();
        } else {
            return value.toString();
        }
    }

    private String constructFilter(KDBColumnHandle column, Domain domain) {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
            } else {
                List<String> conds = new ArrayList<>();
                if (!range.isLowUnbounded() && !range.isHighUnbounded() && column.getKdbType() == KDBType.Date) {
                    long lower = range.isLowInclusive() ? (long) range.getLowValue().get() : (long) range.getLowValue().get()+1;
                    long upper = range.isLowInclusive() ? (long) range.getHighValue().get() : (long) range.getHighValue().get()-1;
                    disjuncts.add(column.getName() + " within " + formatKDBValue(KDBType.Date, lower) + " " + formatKDBValue(KDBType.Date, upper));
                } else {
                    if (!range.isLowUnbounded()) {
                        conds.add(column.getName() + (range.isLowInclusive() ? " >= " : " > ") + formatKDBValue(column.getKdbType(), range.getLowValue().get()));
                    }
                    if (!range.isHighUnbounded()) {
                        conds.add(column.getName() + (range.isHighInclusive() ? " <= " : " < ") + formatKDBValue(column.getKdbType(), range.getHighValue().get()));
                    }
                    if (conds.size() > 1) {
                        disjuncts.add("(" + conds.get(0) + ") & (" + conds.get(1) + ")");
                    } else {
                        disjuncts.add(conds.get(0));
                    }
                }
            }
        }

        if (singleValues.size() == 1) {
            if (column.getKdbType() == KDBType.String) {
                disjuncts.add(column.getName() + " like " + formatKDBValue(KDBType.String, singleValues.get(0)));
            } else {
                disjuncts.add(column.getName() + " = " + formatKDBValue(column.getKdbType(), singleValues.get(0)));
            }
        } else if (singleValues.size() > 1) {
            disjuncts.add(column.getName() + " in (" + String.join("; ", singleValues.stream().map(s -> formatKDBValue(column.getKdbType(),s)).collect(Collectors.toList())) + ")");
        }

        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        } else {
            return disjuncts.stream().map(dis -> "("+dis+")").collect(Collectors.joining(" | "));
        }
    }

    private int columnFilterPriority(KDBColumnHandle col) {
        // top priority to partition column of partitioned tables
        if (col.isPartitionColumn()) return -5;
        // prefer attributes against any columns with attributes
        if (col.getAttribute().isPresent()) return -4;
        if (col.getKdbType() == KDBType.Date) return -3;
        if (col.getKdbType() == KDBType.Symbol) return -2;
        // String comparisons "like" are usually expensive, do them last
        if (col.getKdbType() == KDBType.String) return 1;
        return 0;
    }

    private String constructFilters(TupleDomain<ColumnHandle> domain) {
        if (domain.isAll() || domain.getDomains().isEmpty()) {
            return null;
        }

        TreeMap<KDBColumnHandle, String> conditions = new TreeMap<>(
                (left, right) -> {
                    int leftPriority = columnFilterPriority(left);
                    int rightPriority = columnFilterPriority(right);
                    if (leftPriority != rightPriority) {
                        return Integer.compare(leftPriority, rightPriority);
                    } else {
                        return left.getName().compareTo(right.getName());
                    }
                }
        );

        for (Map.Entry<ColumnHandle, Domain> e : domain.getDomains().get().entrySet()) {
            KDBColumnHandle col = (KDBColumnHandle) e.getKey();
            conditions.put(col, constructFilter(col, e.getValue()));
        }

        return String.join(", ", conditions.values());
    }

    public Page getData(KDBTableHandle handle, List<KDBColumnHandle> columns, int page, int pageSize) throws Exception {
        String table = handle.getTableName();
        String filter = constructFilters(handle.getConstraint());

        // "select count(*) type use cases
        if (columns.isEmpty()) {
            columns = List.of(new KDBColumnHandle("i", BigintType.BIGINT, KDBType.Long, null, false));
        // one more weird exception select date from <partitioned table> where date = <x> gives only a single row
        } else if (columns.size() == 1 && columns.get(0).isPartitionColumn()) {
            columns = List.of(columns.get(0), new KDBColumnHandle("i", BigintType.BIGINT, KDBType.Long, null, false));
        }

        // Flip of column names, column values (arrays)
        String query = "select " + String.join(", ", columns.stream().map(col -> col.getName()).collect(Collectors.toList()))
                + " from " + (handle.isQuery() ? ("("+table+")") : table);
        if (filter != null) {
            query += " where " + filter;
            if (handle.getLimit().isPresent()) {
                query = handle.getLimit().getAsLong() + "#" + query;
            }
        } else {
            if (handle.getLimit().isPresent()) {
                query += " where i<" + handle.getLimit().getAsLong();
            }
        }

        // Pagination
        if (page > 0) {
            query = "select [" + (page*pageSize) + " " + pageSize + "] from " + query;
        } else {
            query = "select [" + pageSize + "] from " + query;
        }

        LOGGER.info("KDB query: "+query);
        c.Flip res = (c.Flip) exec(query);

        PageBuilder builder = new PageBuilder(columns.stream().map(col -> col.getType()).collect(Collectors.toList()));

        builder.declarePositions(getArrayLength(columns.get(0).getType(), res.y[0]));
        for (int i=0; i<columns.size(); i++) {
            BlockBuilder bb = builder.getBlockBuilder(i);
            columns.get(i).getKdbType().writeBlock(bb, res.y[i]);
        }

        return builder.build();
    }

    public TableStatistics getTableStatistics(KDBTableHandle table) throws Exception {
        LOGGER.info("Collecting statistics for table " + table.getSchemaName() + "." + table.getTableName());
        long rows = (long) exec("count " + table.getTableName());

        List<ColumnMetadata> columnMetadata = getTableMeta(table.getTableName());
        String colQuery;
        if (table.isPartitioned()) {
            KDBColumnHandle parCol = table.getPartitionColumn().get();
            // -22 does not work on the whole table, so calculate partition by partition
            colQuery = columnMetadata.stream()
                    .map(col -> "(select name:`" + col.getName() + ", dcount, ncount, size from " +
                            "update size:(+/) {[v] (select count i, size:-22!" + col.getName() + " from "+ table.getTableName() + " where " + parCol.getName() + " = v)[`size]} each (select distinct " + parCol.getName() +" from "+table.getTableName()+")[`" + parCol.getName()+"] from "+
                            "select dcount: `long$(avg dcount) * (count " + table.getTableName() + "), ncount: sum ncount from ((uj/) {[v] select dcount:(count distinct " + col.getName() + ") % (count i), " + ((col.getProperties().get("kdb.type") == KDBType.String) ? "ncount: sum `long$0 = count each " + col.getName(): "ncount: sum `long$null " + col.getName()) + " from " + table.getTableName() + " where " + parCol.getName() + " = v} each (select distinct " + parCol.getName() + " from " + table.getTableName() + ")[`" + parCol.getName() + "])"
                             + ")")
                    .collect(Collectors.joining(" uj "));
        } else {
            colQuery = columnMetadata.stream()
                    .map(col -> "(select name:`" + col.getName() + ", dcount, ncount, size " +
                            "from select dcount:count distinct " + col.getName() + ", " +
                            ((col.getProperties().get("kdb.type") == KDBType.String) ? "ncount: sum `long$0 = count each " + col.getName() + ", " : "ncount: sum `long$null " + col.getName() + ", ") +
                            "size: -22!" + col.getName() + " " +
                            "from " + table.getTableName() + ")")
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
            ColumnMetadata meta = columnMetadata.get(i);
            stats.put(
                    new KDBColumnHandle(meta.getName(), meta.getType(), (KDBType) meta.getProperties().get("kdb.type"), (Optional<KDBAttribute>) meta.getProperties().get("kdb.attribute"), (boolean) meta.getProperties().get("kdb.isPartitionColumn")),
                    new ColumnStatistics(Estimate.of((double) nullCounts[i] / rows), Estimate.of(distinctCounts[i]), Estimate.of(sizes[i]), Optional.empty()));
        }

        return new TableStatistics(Estimate.of(rows), stats);
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

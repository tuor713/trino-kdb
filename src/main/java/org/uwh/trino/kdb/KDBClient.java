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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import kx.c;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class KDBClient {
    private static final Logger LOGGER = Logger.get(KDBClient.class);
    private final String host;
    private final int port;
    private String user;
    private String password;
    private final kx.c connection;

    public KDBClient(String host, int port, String user, String password) throws Exception {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        if (user != null && password != null) {
            connection = new c(host, port, user + ":" + password);
        } else {
            connection = new c(host, port);
        }
    }

    public List<String> listTables() throws Exception {
        String[] res = (String[]) connection.k("system \"a\"");
        return Arrays.asList(res);
    }

    public List<ColumnMetadata> getTableMeta(String name) throws Exception {
        c.Dict res = (c.Dict) connection.k("meta "+name);
        c.Flip columns = (c.Flip) res.x;
        c.Flip colMeta = (c.Flip) res.y;
        String[] colNames = (String[]) columns.y[0];
        char[] types = (char[]) colMeta.y[0];

        List<ColumnMetadata> result = new ArrayList<>();
        for (int i=0; i<colNames.length; i++) {
            KDBType kdbType = KDBType.fromTypeCode(types[i]);
            ColumnMetadata col = new ColumnMetadata(colNames[i], kdbType.getTrinoType(), null, null,false, Map.of("kdb.type", kdbType));
            result.add(col);
        }

        return ImmutableList.copyOf(result);
    }

    private String formatKDBValue(KDBType type, Object value) {
        if (type == KDBType.Date) {
            LocalDate date = LocalDate.ofEpochDay((long) value);
            return date.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        } else if (value instanceof Slice) {
            Slice s = (Slice) value;
            return "`" + s.toStringUtf8();
        } else {
            return value.toString();
        }
    }

    private String constructFilter(KDBMetadata.KDBColumnHandle column, Domain domain) {
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
                disjuncts.add(column.getName() + " like \"" + ((Slice) singleValues.get(0)).toStringUtf8() + "\"");
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

    private String constructFilters(TupleDomain<ColumnHandle> domain) {
        if (domain.isAll() || domain.getDomains().isEmpty()) {
            return null;
        }

        TreeMap<KDBMetadata.KDBColumnHandle, String> conditions = new TreeMap<KDBMetadata.KDBColumnHandle, String>(
                Comparator.comparingInt(col -> {
                    if (col.getKdbType() == KDBType.Date) {
                        if (col.getName().equals("date")) {
                            return -4;
                        } else {
                            return -3;
                        }
                    } else if (col.getKdbType() == KDBType.Symbol) {
                        return -2;
                    } else if (col.getKdbType() == KDBType.String) {
                        return 1;
                    } else {
                        return 0;
                    }
                })
        );

        for (Map.Entry<ColumnHandle, Domain> e : domain.getDomains().get().entrySet()) {
            KDBMetadata.KDBColumnHandle col = (KDBMetadata.KDBColumnHandle) e.getKey();
            conditions.put(col, constructFilter(col, e.getValue()));
        }

        return String.join(", ", conditions.values());
    }

    public Page getData(KDBTableHandle handle, List<KDBMetadata.KDBColumnHandle> columns, int page, int pageSize) throws Exception {
        String table = handle.getTableName();
        String filter = constructFilters(handle.getConstraint());

        // "select count(*) type use cases
        if (columns.isEmpty()) {
            columns = List.of(new KDBMetadata.KDBColumnHandle("i", BigintType.BIGINT, KDBType.Long));
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
        c.Flip res = (c.Flip) connection.k(query);

        PageBuilder builder = new PageBuilder(columns.stream().map(col -> col.getType()).collect(Collectors.toList()));

        builder.declarePositions(getArrayLength(columns.get(0).getType(), res.y[0]));
        for (int i=0; i<columns.size(); i++) {
            BlockBuilder bb = builder.getBlockBuilder(i);
            columns.get(i).getKdbType().writeBlock(bb, res.y[i]);
        }

        return builder.build();
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

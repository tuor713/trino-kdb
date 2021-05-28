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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    private String formatKDBValue(Type type, Object value) {
        if (value instanceof Slice) {
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
                if (!range.isLowUnbounded()) {
                    conds.add(column.getName() + (range.isLowInclusive() ? " >= " : " > ") + range.getLowValue().get());
                }
                if (!range.isHighUnbounded()) {
                    conds.add(column.getName() + (range.isHighInclusive() ? " <= " : " < ") + range.getHighValue().get());
                }
                if (conds.size() > 1) {
                    disjuncts.add("(" + conds.get(0) + ") & (" + conds.get(1) + ")");
                } else {
                    disjuncts.add(conds.get(0));
                }
            }
        }

        if (singleValues.size() == 1) {
            disjuncts.add(column.getName() + " = " + formatKDBValue(column.getType(),singleValues.get(0)));
        } else if (singleValues.size() > 1) {
            disjuncts.add(column.getName() + " in (" + String.join("; ", singleValues.stream().map(s -> formatKDBValue(column.getType(),s)).collect(Collectors.toList())) + ")");
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

        List<String> conditions = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> e : domain.getDomains().get().entrySet()) {
            conditions.add(constructFilter((KDBMetadata.KDBColumnHandle) e.getKey(), e.getValue()));
        }

        return String.join(", ", conditions);
    }

    public Page getData(KDBTableHandle handle, List<KDBMetadata.KDBColumnHandle> columns) throws Exception {
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

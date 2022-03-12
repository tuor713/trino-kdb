package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KDBTableHandle implements ConnectorTableHandle {
    private final String namespace;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    private final boolean isPartitioned;
    // Year represented as yyyy, month as yyyy.MM, date as yyyy.MM.dd inline with KDB representation
    private final List<String> partitions;
    private final Optional<KDBColumnHandle> partitionColumn;
    private final List<KDBFilter> extraFilters;

    @JsonCreator
    public KDBTableHandle(@JsonProperty("namespace") String namespace,
                          @JsonProperty("tableName") String tableName,
                          @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
                          @JsonProperty("limit") OptionalLong limit,
                          @JsonProperty("isPartitioned") boolean isPartitioned,
                          @JsonProperty("partitionColumn") Optional<KDBColumnHandle> partitionColumn,
                          @JsonProperty("partitions") List<String> partitions,
                          @JsonProperty("extraFilters") List<KDBFilter> extraFilters) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.constraint = constraint;
        this.limit = limit;
        this.isPartitioned = isPartitioned;
        this.partitionColumn = partitionColumn;
        this.partitions = partitions;
        this.extraFilters = extraFilters;
    }

    @JsonProperty
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    public String getQualifiedTableName() {
        if (namespace.equals("")) {
            return tableName;
        } else {
            return "." + namespace + "." + tableName;
        }
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() { return constraint; }

    @JsonProperty
    public OptionalLong getLimit() { return limit; }

    @JsonProperty("isPartitioned")
    public boolean isPartitioned() {
        return isPartitioned;
    }

    @JsonProperty("partitions")
    public List<String> getPartitions() {
        return partitions;
    }

    @JsonProperty("partitionColumn")
    public Optional<KDBColumnHandle> getPartitionColumn() {
        return partitionColumn;
    }

    @JsonProperty("extraFilters")
    public List<KDBFilter> getExtraFilters() { return extraFilters; }

    public boolean isQuery() {
        return isQuery(tableName);
    }

    public static boolean isQuery(String tableName) {
        return !tableName.matches("[a-zA-Z.][a-zA-Z._0-9]*");
    }

    public String toQuery(List<KDBColumnHandle> columns, OptionalInt page, int pageSize, boolean isVirtualTables) {
        StringBuilder query = new StringBuilder();

        // Pagination & Limits
        if (page.isPresent() || limit.isPresent()) {
            long startIndex = page.orElse(0) * pageSize;
            long endIndex = Math.min(page.isPresent() ? (page.getAsInt() + 1) * pageSize : Long.MAX_VALUE, limit.orElse(Long.MAX_VALUE));

            if (startIndex > 0) {
                query.append("select [").append(startIndex).append(" ").append(endIndex - startIndex).append("] ");
            } else {
                query.append("select [").append(endIndex).append("] ");
            }

            // partitioned tables don't directly support paging -> create an inner query first
            if (isPartitioned || isVirtualTables) {
                query.append("from select ");
            }
        } else {
            query.append("select ");
        }

        if (!columns.isEmpty()) {
            query.append(columns.stream().map(KDBColumnHandle::getName).collect(Collectors.joining(", "))).append(" ");
        }

        query.append("from ").append(getTableNameQuery());

        getWhereClause().ifPresent(s -> query.append(" where ").append(s));

        return query.toString();
    }

    public String getTableNameQuery() {
        // queries need to wrapped, unless they already are
        if (isQuery() && !tableName.matches("^\\(.*\\)$")) {
            return "("+tableName+")";
        } else {
            return getQualifiedTableName();
        }
    }

    public Optional<String> getWhereClause() {
        String filter = constructFilters(constraint, extraFilters);
        if (filter != null) {
            return Optional.of(filter);
        } else {
            // optimization to limit directly on row index
            if (limit.isPresent()) {
                return Optional.of("i<" + limit.getAsLong());
            }
        }

        return Optional.empty();
    }

    // https://code.kx.com/q/basics/syntax/#names-and-namespaces
    private static final Predicate<String> VALID_NAME = Pattern.compile("^[.a-zA-Z][._a-zA-Z0-9]*$").asPredicate();

    private static String formatKDBValue(KDBType type, Object value) {
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
            return VALID_NAME.test(s.toStringUtf8()) ? "`" + s.toStringUtf8() : "`$\"" + s.toStringUtf8() + "\"";
        } else {
            return value.toString();
        }
    }

    private static String constructFilter(KDBColumnHandle column, Domain domain) {
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
                        if (column.getKdbType() != KDBType.String) {
                            conds.add(column.getName() + (range.isLowInclusive() ? " >= " : " > ") + formatKDBValue(column.getKdbType(), range.getLowValue().get()));
                        } else {
                            // Strings don't support comparison, cast to symbol instead
                            // https://stackoverflow.com/questions/57176867/compare-if-one-string-is-greater-than-another-in-kdb
                            conds.add(
                                    "(`$"+column.getName() + ")" +
                                    (range.isLowInclusive() ? " >= " : " > ") +
                                    formatKDBValue(KDBType.Symbol, range.getLowValue().get())
                            );
                        }
                    }
                    if (!range.isHighUnbounded()) {
                        if (column.getKdbType() != KDBType.String) {
                            conds.add(column.getName() + (range.isHighInclusive() ? " <= " : " < ") + formatKDBValue(column.getKdbType(), range.getHighValue().get()));
                        } else {
                            conds.add(
                                    "(`$" + column.getName() + ")" +
                                    (range.isHighInclusive() ? " <= " : " < ") +
                                    formatKDBValue(KDBType.Symbol, range.getHighValue().get())
                            );
                        }
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

    private static int columnFilterPriority(KDBColumnHandle col) {
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

    public static String constructFilters(TupleDomain<ColumnHandle> domain, List<KDBFilter> extraFilters) {
        if (domain.isAll() && extraFilters.isEmpty()) {
            return null;
        } else if (domain.isNone()) {
            // impossible constraint
            return "i = -1";
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

        for (KDBFilter e : extraFilters) {
            String filter = e.toKDBFilter();
            if (conditions.containsKey(e.getColumn())) {
                conditions.put(e.getColumn(), conditions.get(e.getColumn()) + ", " + filter);
            } else {
                conditions.put(e.getColumn(), filter);
            }
        }

        return String.join(", ", conditions.values());
    }

    private static String constructLikeFilter(KDBColumnHandle column, String pattern) {
        return column.getName() + " like \"" + pattern + "\"";
    }

    @Override
    public String toString() {
        return "KDBTableHandle{" +
                "namespace='" + namespace + '\'' +
                ", tableName='" + tableName + '\'' +
                ", constraint=" + constraint +
                ", limit=" + limit +
                ", isPartitioned=" + isPartitioned +
                ", partitions=" + partitions +
                ", partitionColumn=" + partitionColumn +
                ", extraFilters=" + extraFilters +
                '}';
    }
}

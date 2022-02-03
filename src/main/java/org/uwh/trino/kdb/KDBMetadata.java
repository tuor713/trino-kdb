package org.uwh.trino.kdb;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KDBMetadata implements ConnectorMetadata {
    private static final Logger LOGGER = Logger.get(KDBMetadata.class);
    private static final String SCHEMA_NAME = "default";
    private static final String DEFAULT_NS = "";
    private final KDBClient client;
    private final boolean useStats;
    private final StatsManager stats;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Map<String,String> schemaMetadataCache = new HashMap<>();
    private final Map<SchemaTableName,String> tableMetadataCache = new HashMap<>();
    private final Cache<String,List<ColumnMetadata>> columnMetadataCache;

    public KDBMetadata(KDBClient client, Config config, StatsManager stats) {
        this.client = client;
        this.useStats = config.useStats();
        this.stats = stats;
        executor.scheduleAtFixedRate(this::refreshMetadata, 0, config.getMetadataRefreshInterval(), TimeUnit.SECONDS);
        columnMetadataCache = CacheBuilder.newBuilder().expireAfterWrite(config.getMetadataRefreshInterval(), TimeUnit.SECONDS).build();
    }

    private void refreshMetadata() {
        try {
            client.listNamespaces().stream().forEach( ns -> {
                schemaMetadataCache.put(ns.toLowerCase(Locale.ENGLISH), ns);
            });
            client.listTables().stream().forEach( st -> {
                tableMetadataCache.put(new SchemaTableName(resolveSchema(st[0]), st[1]), st[1]);
            });
        } catch (Exception e) {
            LOGGER.warn(e, "Failed to refresh KDB metadata from instance: " + client.getHost() + ":" + client.getPort());
        }
    }

    private List<ColumnMetadata> getColumns(KDBTableHandle handle) {
        try {
            return columnMetadataCache.get(handle.getQualifiedTableName(), () -> {
                return client.getTableMeta(handle);
            });
        } catch (ExecutionException e) {
            LOGGER.error("Could not retrieve metadata for table "+handle.getQualifiedTableName());
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return schemaMetadataCache.keySet().stream().map(s -> s.isEmpty() ? SCHEMA_NAME : s).collect(Collectors.toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        try {
            String ns = resolveKDBNamespace(schemaName);
            final String schema = resolveSchema(ns);
            return client.listTables(ns).stream().map(t -> new SchemaTableName(schema, t)).collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String resolveSchema(String ns) {
        if (ns.equals(DEFAULT_NS)) {
            return SCHEMA_NAME;
        } else {
            return ns.toLowerCase(Locale.ENGLISH);
        }
    }

    private String resolveKDBNamespace(Optional<String> schemaName) {
        String ns = DEFAULT_NS;
        if (schemaName.isPresent() && !schemaName.get().equals(SCHEMA_NAME)) {
            ns = schemaMetadataCache.getOrDefault(schemaName.get(), schemaName.get());
        }
        return ns;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        try {
            return client.listTables()
                    .stream()
                    .filter(t -> prefix.getTable().stream().allMatch(tname -> tname.equals(t[1])))
                    .filter(t -> prefix.getSchema().stream().allMatch(sname -> sname.equals(resolveSchema(t[0]))))
                    .map(t -> new SchemaTableName(resolveSchema(t[0]), t[1]))
                    .collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getColumns((KDBTableHandle) getTableHandle(session, tableName)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return columns.build();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        try {
            String tName = tableName.getTableName();
            // Retrieve original capitalization since KDB is case sensitive
            if (tableMetadataCache.containsKey(tableName)) {
                tName = tableMetadataCache.get(tableName);
            }

            if (KDBTableHandle.isQuery(tName) && tName.contains("\\")) {
                tName = unescapeDynamicQuery(tName);
            }

            return client.getTableHandle(resolveKDBNamespace(Optional.of(tableName.getSchemaName())), tName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String unescapeDynamicQuery(String name) {
        StringBuilder res = new StringBuilder();
        boolean escape = false;
        for (char c : name.toCharArray()) {
            if (c == '\\') {
                if (escape) {
                    escape = false;
                    res.append('\\');
                } else {
                    escape = true;
                }
            } else {
                if (escape) {
                    res.append(Character.toUpperCase(c));
                    escape = false;
                } else {
                    res.append(c);
                }
            }
        }

        return res.toString();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        KDBTableHandle handle = (KDBTableHandle) table;
        try {
            List<ColumnMetadata> columns = getColumns(handle);
            return new ConnectorTableMetadata(new SchemaTableName(resolveSchema(handle.getNamespace()), handle.getTableName()), columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ImmutableMap.Builder<String,ColumnHandle> builder = ImmutableMap.builder();
        KDBTableHandle handle = (KDBTableHandle) tableHandle;
        try {
            List<ColumnMetadata> columns = getColumns(handle);
            columns.forEach(col -> builder.put(col.getName(),
                    new KDBColumnHandle(
                            // use the original capitalization
                            (String) col.getProperties().get("kdb.name"),
                            col.getType(),
                            (KDBType) col.getProperties().get("kdb.type"),
                            (Optional<KDBAttribute>) col.getProperties().get("kdb.attribute"),
                            (boolean) col.getProperties().get("kdb.isPartitionColumn"))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        KDBColumnHandle handle = (KDBColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getName(), handle.getType());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit) {
        KDBTableHandle khandle = (KDBTableHandle) handle;
        if (khandle.getLimit().isPresent() && khandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new KDBTableHandle(khandle.getNamespace(), khandle.getTableName(), khandle.getConstraint(), OptionalLong.of(limit), khandle.isPartitioned(), khandle.getPartitionColumn(), khandle.getPartitions()),
                // for partitioned table since partitions are limited individually the limit is not guaranteed
                !khandle.isPartitioned(),
                false
        ));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        KDBTableHandle khandle = (KDBTableHandle) handle;
        TupleDomain<ColumnHandle> current = khandle.getConstraint();
        TupleDomain<ColumnHandle> next = current.intersect(constraint.getSummary());
        if (current.equals(next)) {
            return Optional.empty();
        }

        KDBTableHandle newHandle = new KDBTableHandle(khandle.getNamespace(), khandle.getTableName(), next, khandle.getLimit(), khandle.isPartitioned(), khandle.getPartitionColumn(), khandle.getPartitions());

        return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.all(), false));
    }

    private static final Map<String,String> supported_functions = ImmutableMap.<String,String>builder()
            .putAll(Map.of(
                    "count", "count",
                    "sum", "sum",
                    "avg", "avg",
                    "max", "max",
                    "min", "min",
                    "stddev", "sdev",
                    "stddev_pop", "dev",
                    "variance", "svar",
                    "var_pop", "var",
                    "bool_and", "all"))
            .putAll(Map.of(
                    "bool_or", "any",
                    "count_if", "sum `long$"
            ))
            .build();

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle ihandle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets) {
        if (!session.getProperty(Config.SESSION_PUSH_DOWN_AGGREGATION, Boolean.class)) {
            return Optional.empty();
        }

        KDBTableHandle handle = (KDBTableHandle) ihandle;

        if (!aggregates.stream().allMatch(agg -> supported_functions.containsKey(agg.getFunctionName()))) {
            return Optional.empty();
        }

        // Only support single grouping set
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        List<ConnectorExpression> projections = new ArrayList<>();
        StringBuilder newQuery = new StringBuilder();
        newQuery.append("select ");
        for (int i=0; i<aggregates.size(); i++) {
            if (i>0) {
                newQuery.append(", ");
            }
            newQuery.append("col").append(i).append(": ");
            AggregateFunction func = aggregates.get(i);

            // not supported yet
            if (func.isDistinct()) {
                return Optional.empty();
            }

            newQuery.append(supported_functions.get(func.getFunctionName())).append(" ");

            projections.add(new Variable("col"+i, func.getOutputType()));

            if (func.getInputs().size() == 0) {
                // count(*) use case
                newQuery.append("i");
            } else if (func.getInputs().size() == 1) {
                ConnectorExpression expr = func.getInputs().get(0);
                if (expr instanceof Variable) {
                    Variable var = (Variable) expr;
                    KDBColumnHandle colHandle = (KDBColumnHandle) assignments.get(var.getName());
                    if (colHandle == null) {
                        Optional.empty();
                    }
                    newQuery.append(colHandle.getName());
                } else {
                    return Optional.empty();
                }
            } else {
                // can't handle multiple args
                return Optional.empty();
            }
        }

        if (aggregates.isEmpty()) {
            newQuery.append("count i");
        }

        List<KDBColumnHandle> grouping = (List) groupingSets.get(0);

        if (!grouping.isEmpty()) {
            newQuery.append(" by ");
            newQuery.append(grouping.stream().map(h -> h.getName()).collect(Collectors.joining(", ")));
        }

        newQuery.append(" from ");

        // limit and constraint -> need to construct a sub-query before running aggregation
        if (handle.getLimit().isPresent() && !handle.getConstraint().isAll()) {
            newQuery.append("(")
                    .append(handle.toQuery(Collections.emptyList(), OptionalInt.empty(), 50000))
                    .append(")");
        } else {
            newQuery.append(handle.getTableNameQuery());
            handle.getWhereClause().ifPresent(s -> newQuery.append(" where ").append(s));
        }

        AggregationApplicationResult<ConnectorTableHandle> result = new AggregationApplicationResult<>(
                new KDBTableHandle(DEFAULT_NS, newQuery.toString(), TupleDomain.all(), OptionalLong.empty(), false, Optional.empty(), List.of()),
                projections,
                projections.stream().map(v -> {
                    Variable var = (Variable) v;
                    return new Assignment(
                            var.getName(),
                            new KDBColumnHandle(var.getName(), var.getType(), KDBType.fromTrinoType(var.getType()), Optional.empty(), false),
                            var.getType());
                }).collect(Collectors.toList()),
                Map.of(),
                false
        );

        return Optional.of(result);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        KDBTableHandle khandle = (KDBTableHandle) handle;

        if (!session.getProperty(Config.SESSION_USE_STATS, Boolean.class) || khandle.isQuery()) {
            return TableStatistics.empty();
        }

        try {
            return stats.getTableStats(khandle);
        } catch (Exception e) {
            return TableStatistics.empty();
        }
    }
}

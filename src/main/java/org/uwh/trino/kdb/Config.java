package org.uwh.trino.kdb;

import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;

public class Config {
    public static final String KDB_HOST_KEY = "kdb.host";
    public static final String KDB_PORT_KEY = "kdb.port";
    public static final String KDB_USER_KEY = "kdb.user";
    public static final String KDB_PASSWORD_KEY = "kdb.password";

    public static final String KDB_PAGE_SIZE = "page.size";
    public static final String DEFAULT_PAGE_SIZE = "50000";
    public static final String SESSION_PAGE_SIZE = "page_size";

    public static final String KDB_USE_STATS_KEY = "use.stats";
    public static final String DEFAULT_USE_STATS = "true";
    public static final String SESSION_USE_STATS = "use_stats";

    public static final String KDB_DYNAMIC_STATS = "dynamic.stats";
    public static final String DEFAULT_DYNAMIC_STATS = "false";
    public static final String SESSION_DYNAMIC_STATS = "use_dynamic_stats";


    public static final String KDB_METADATA_REFRESH_INTERVAL = "kdb.metadata.refresh.interval.seconds";
    public static final String DEFAULT_METADATA_REFRESH_INTERVAL = String.valueOf(60*60); // 1 hour

    public static final String KDB_PUSH_DOWN_AGGREGATION = "push.down.aggregation";
    public static final String DEFAULT_PUSH_DOWN_AGGREGATION = "true";
    public static final String SESSION_PUSH_DOWN_AGGREGATION = "push_down_aggregation";

    public static final String KDB_VIRTUAL_TABLES = "virtual.tables";
    public static final String DEFAULT_VIRTUAL_TABLES = "false";
    public static final String SESSION_VIRTUAL_TABLES = "virtual_tables";

    public static final String KDB_INSERT_FUNCTION = "insert.function";
    public static final String DEFAULT_INSERT_FUNCTION = "insert";
    public static final String SESSION_INSERT_FUNCTION = "insert_function";

    public static final String KDB_PUSH_DOWN_LIKE = "push.down.like";
    public static final String DEFAULT_PUSH_DOWN_LIKE = "false";
    public static final String SESSION_PUSH_DOWN_LIKE = "push_down_like";

    private final Map<String,String> config;

    public Config(Map<String,String> config) {
        this.config = config;
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return List.of(
                PropertyMetadata.booleanProperty(SESSION_PUSH_DOWN_AGGREGATION, "Push down aggregations into KDB", pushDownAggregation(), false),
                PropertyMetadata.booleanProperty(SESSION_USE_STATS, "Use statistics", useStats(), false),
                PropertyMetadata.booleanProperty(SESSION_DYNAMIC_STATS, "Calculate stats dynamically for tables without pre-generated stats", useDynamicStats(),false),
                PropertyMetadata.integerProperty(SESSION_PAGE_SIZE, "Page size for sourcing KDB data in rows", getPageSize(), false),
                PropertyMetadata.booleanProperty(SESSION_VIRTUAL_TABLES, "Treat tables as virtual tables", getVirtualTables(), false),
                PropertyMetadata.stringProperty(SESSION_INSERT_FUNCTION, "Insert function to use in KDB", getInsertFunction(), false),
                PropertyMetadata.booleanProperty(SESSION_PUSH_DOWN_LIKE, "Push down like filter expressions", pushDownLike(), false)
        );
    }


    public String getHost() {
        return config.get(KDB_HOST_KEY);
    }

    public int getPort() {
        return Integer.parseInt(config.get(KDB_PORT_KEY));
    }

    public String getUser() {
        return config.get(KDB_USER_KEY);
    }

    public String getPassword() {
        return config.get(KDB_PASSWORD_KEY);
    }

    public int getPageSize() {
        return Integer.parseInt(config.getOrDefault(KDB_PAGE_SIZE, DEFAULT_PAGE_SIZE));
    }

    public boolean useStats() {
        return Boolean.parseBoolean(config.getOrDefault(KDB_USE_STATS_KEY, DEFAULT_USE_STATS));
    }

    public boolean useDynamicStats() {
        return Boolean.parseBoolean(config.getOrDefault(KDB_DYNAMIC_STATS, DEFAULT_DYNAMIC_STATS));
    }

    public boolean pushDownAggregation() {
        return Boolean.parseBoolean(config.getOrDefault(KDB_PUSH_DOWN_AGGREGATION, DEFAULT_PUSH_DOWN_AGGREGATION));
    }

    public int getMetadataRefreshInterval() {
        return Integer.parseInt(config.getOrDefault(KDB_METADATA_REFRESH_INTERVAL, DEFAULT_METADATA_REFRESH_INTERVAL));
    }

    public boolean getVirtualTables() {
        return Boolean.parseBoolean(config.getOrDefault(KDB_VIRTUAL_TABLES, DEFAULT_VIRTUAL_TABLES));
    }

    public String getInsertFunction() {
        return config.getOrDefault(KDB_INSERT_FUNCTION, DEFAULT_INSERT_FUNCTION);
    }

    public boolean pushDownLike() {
        return Boolean.parseBoolean(config.getOrDefault(KDB_PUSH_DOWN_LIKE, DEFAULT_PUSH_DOWN_LIKE));
    }
}

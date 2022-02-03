package org.uwh.trino.kdb;

import io.airlift.log.Logger;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.TableStatistics;

import java.util.HashMap;
import java.util.Map;

public class StatsManager {
    private static final Logger LOGGER = Logger.get(StatsManager.class);

    private final KDBClient client;
    private final Map<SchemaTableName, TableStatistics> cachedStats = new HashMap<>();

    public StatsManager(KDBClient client) {
        this.client = client;
    }

    public TableStatistics getTableStats(KDBTableHandle table) {
        SchemaTableName fname = new SchemaTableName(KDBMetadata.resolveSchema(table.getNamespace()), table.getTableName());
        if (!cachedStats.containsKey(fname)) {
            try {
                cachedStats.put(fname, client.getTableStatistics(table));
            } catch (Exception e) {
                LOGGER.warn(e, "Exception collecting stats for table "+table);
                return TableStatistics.empty();
            }
        }
        return cachedStats.get(fname);
    }
}

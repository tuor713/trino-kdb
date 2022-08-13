package org.uwh.trino.kdb;

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.TableStatistics;

import java.util.HashMap;
import java.util.Map;

public class StatsManager {
    private static final Logger LOGGER = Logger.get(StatsManager.class);

    private final KDBClientFactory factory;
    private final Map<SchemaTableName, TableStatistics> cachedStats = new HashMap<>();

    public StatsManager(KDBClientFactory factory) {
        this.factory = factory;
    }

    public TableStatistics getTableStats(KDBTableHandle table, ConnectorSession session, boolean calcStatsOnTheFly) {
        SchemaTableName fname = new SchemaTableName(KDBMetadata.resolveSchema(table.getNamespace()), table.getTableName());
        KDBClient client = factory.getClient(session.getIdentity());
        if (!cachedStats.containsKey(fname)) {
            try {
                cachedStats.put(fname, client.getTableStatistics(table, calcStatsOnTheFly));
            } catch (Exception e) {
                LOGGER.warn(e, "Exception collecting stats for table "+table);
                return TableStatistics.empty();
            }
        }
        return cachedStats.get(fname);
    }
}

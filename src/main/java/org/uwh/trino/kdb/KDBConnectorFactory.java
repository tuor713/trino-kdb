package org.uwh.trino.kdb;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;

import java.util.Map;

public class KDBConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "kdb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new KDBHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Config cfg = new Config(config);

        try {
            KDBClient client = new KDBClient(cfg.getHost(), cfg.getPort(), cfg.getUser(), cfg.getPassword());
            return new KDBConnector(client, cfg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

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
        String host = config.get("kdb.host");
        int port = Integer.parseInt(config.get("kdb.port"));
        String user = config.get("kdb.user");
        String password = config.get("kdb.password");

        try {
            KDBClient client = new KDBClient(host, port, user, password);
            return new KDBConnector(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

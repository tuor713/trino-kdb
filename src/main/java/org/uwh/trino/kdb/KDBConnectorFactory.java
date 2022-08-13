package org.uwh.trino.kdb;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class KDBConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "kdb";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Config cfg = new Config(config);

        try {
            KDBClientFactory factory = new KDBClientFactory(
                    cfg.getHost(),
                    cfg.getPort(),
                    cfg.getUser(),
                    cfg.getPassword(),
                    cfg.getExtraCredentialUser(),
                    cfg.getExtraCredentialPassword());
            return new KDBConnector(factory, cfg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

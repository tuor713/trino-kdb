package org.uwh.trino.kdb;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

public class KDBPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new KDBConnectorFactory());
    }
}

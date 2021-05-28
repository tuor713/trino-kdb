package org.uwh.trino.kdb;

import java.util.Map;

public class Config {
    public static final String KDB_HOST_KEY = "kdb.host";
    public static final String KDB_PORT_KEY = "kdb.port";
    public static final String KDB_USER_KEY = "kdb.user";
    public static final String KDB_PASSWORD_KEY = "kdb.password";
    public static final String KDB_PAGE_SIZE = "page.size";
    public static final String DEFAULT_PAGE_SIZE = "50000";

    private final Map<String,String> config;

    public Config(Map<String,String> config) {
        this.config = config;
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
}

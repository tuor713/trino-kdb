package org.uwh.trino.kdb;

import io.trino.spi.security.ConnectorIdentity;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class KDBClientFactory {
    private final String host;
    private final int port;
    private final String defaultUser;
    private final String defaultPassword;
    private final Optional<String> extraUserKey;
    private final Optional<String> extraPasswordKey;

    private final ConcurrentMap<String,KDBClient> clientCache;

    private final MessageDigest sha256;

    public KDBClientFactory(String host, int port, String user, String password, Optional<String> extraUserKey, Optional<String> extraPasswordKey) {
        this.host = host;
        this.port = port;
        this.defaultUser = user;
        this.defaultPassword = password;
        this.extraUserKey = extraUserKey;
        this.extraPasswordKey = extraPasswordKey;
        this.clientCache = new ConcurrentHashMap<>();
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public KDBClient getClient(ConnectorIdentity identity) {
        String finalUser = extraUserKey.flatMap(k -> Optional.ofNullable(identity.getExtraCredentials().get(k))).orElse(defaultUser);
        String finalPassword = extraPasswordKey.flatMap(k -> Optional.ofNullable(identity.getExtraCredentials().get(k))).orElse(defaultPassword);
        return getClient(finalUser, finalPassword);
    }

    public KDBClient getDefaultClient() {
        return getClient(defaultUser, defaultPassword);
    }

    private KDBClient getClient(String user, String password) {
        return clientCache.computeIfAbsent(hash(user+":"+password), k -> {
            try {
                return new KDBClient(host, port, user, password);
            } catch (Exception e) {
                throw new RuntimeException("Could not establish KDB connection", e);
            }
        });
    }

    private synchronized String hash(String value)
    {
        return new String(sha256.digest(value.getBytes(UTF_8)));
    }
}

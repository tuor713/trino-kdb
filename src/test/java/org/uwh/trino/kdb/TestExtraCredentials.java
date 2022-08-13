package org.uwh.trino.kdb;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestExtraCredentials extends AbstractTestQueryFramework {

    @Test
    public void canConnect() {
        MaterializedResult res = computeActual("select count(*) from \"([] sym: `a`a`b)\"");
        assertEquals(res.getOnlyValue(), 3L);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        DistributedQueryRunner qrunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(1)
                .setExtraProperties(ImmutableMap.of())
                .build();

        qrunner.installPlugin(new KDBPlugin());
        qrunner.createCatalog("kdb", "kdb",
                ImmutableMap.<String,String>builder()
                        .put("kdb.host", "localhost")
                        .put("kdb.port", "8000")
                        .put("kdb.user", "wrong_user")
                        .put("kdb.password", "wrong_password")
                        .put("kdb.extra.credential.user","kdbuser")
                        .put("kdb.extra.credential.password", "kdbpassword")
                        .build());

        return qrunner;
    }

    private static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("kdb")
                .setSchema(schema)
                .setIdentity(new Identity.Builder("wrong_user")
                        .withExtraCredentials(Map.of(
                                "kdbuser", "user",
                                "kdbpassword", "password"
                        ))
                        .build())
                .build();
    }
}

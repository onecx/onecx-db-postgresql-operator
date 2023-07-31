package io.github.onecx.operator.db.postgresql;

import static io.github.onecx.operator.db.postgresql.PostgreSQLReconciler.HOST;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(PostgreSQLSchemaNoGrantUserRoleAdminTest.CustomProfile.class)
class PostgreSQLSchemaNoGrantUserRoleAdminTest {

    final static Logger log = Logger.getLogger(PostgreSQLSchemaNoGrantUserRoleAdminTest.class);

    @Inject
    Operator operator;

    @Inject
    KubernetesClient client;

    @BeforeAll
    public static void init() {
        Awaitility.setDefaultPollDelay(2, SECONDS);
        Awaitility.setDefaultPollInterval(2, SECONDS);
        Awaitility.setDefaultTimeout(10, SECONDS);
    }

    private static Stream<Arguments> provideDatabaseSpecForTest() {
        return Stream.of(
                Arguments.of("no-grant-test-1",
                        create("no_grant_test_database", "no_grant_test_user", "no_grant_test_user", "pk", "no-grant-test-db-1",
                                List.of("seg", "cube"), null),
                        "no_grant_test_password", "no_grant_test_user"));
    }

    private static DatabaseSpec create(String database, String user, String schema, String passwordKey, String passwordSecret,
            List<String> extensions, String userSearchPath) {
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(database);
        spec.setUser(user);
        spec.setHost(HOST);
        spec.setSchema(schema);
        spec.setPasswordKey(passwordKey);
        spec.setExtensions(extensions);
        spec.setPasswordSecrets(passwordSecret);
        spec.setUserSearchPath(userSearchPath);
        return spec;
    }

    @ParameterizedTest
    @MethodSource("provideDatabaseSpecForTest")
    void databaseSpecTests(String name, DatabaseSpec spec, String testPassword, String checkSchema) {

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client.getNamespace()).build());
        database.setSpec(spec);

        Secret secret = new Secret();
        secret.setMetadata(new ObjectMetaBuilder().withName(spec.getPasswordSecrets())
                .withNamespace(client.getNamespace()).build());
        secret.setData(Map.of(spec.getPasswordKey(), encoder.encodeToString(testPassword.getBytes())));

        log.infof("Creating test database object: %s", database);
        client.resource(database).serverSideApply();

        log.infof("Creating test secret object: %s", secret);
        client.resource(secret).serverSideApply();

        log.info("Waiting max 10 seconds for expected database resources to be created and updated");

        await().untilAsserted(() -> {
            Assertions.assertDoesNotThrow(() -> {
                try (Connection con = createConnection(spec.getUser(), testPassword, spec.getName())) {
                    log.infof("Create connection to database %s and schema %s", spec.getName(), con.getSchema());
                    if (!checkSchema.equals(con.getSchema())) {
                        throw new Exception(
                                "Wrong connection schema '" + con.getSchema() + "' expected '" + checkSchema + "'");
                    }
                    log.infof("Schema created: %s", con.getSchema());
                }
            });
        });

    }

    private static Connection createConnection(String user, String password, String database) throws Exception {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);

        Config config = ConfigProvider.getConfig();
        String defaultUrl = config.getValue("quarkus.datasource.jdbc.url", String.class);
        Driver driver = DriverManager.getDriver(defaultUrl);
        String defaultDatabase = config.getValue("quarkus.datasource.username", String.class);
        String url = defaultUrl.replace(defaultDatabase, database);
        log.infof("Create JDBC test connection: %s", url);
        return driver.connect(url, properties);
    }

    public static class CustomProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("onecx.operator.db.postgresql.grant-user-role-to-admin", "false");
        }
    }
}

package io.github.onecx.operator.db.postgresql;

import static io.github.onecx.operator.db.postgresql.PostgreSQLReconciler.HOST;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class PostgreSQLSchemaTest {

    final static Logger log = Logger.getLogger(PostgreSQLSchemaTest.class);

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

    @Test
    void createUserDatabaseAndUserSchema() {
        String testUser = "test_user";
        String testPassword = "test_password";
        String testDatabase = "test_database";
        String testSchema = "test_user";

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(testDatabase);
        spec.setUser(testUser);
        spec.setHost(HOST);
        spec.setSchema(testSchema);
        spec.setPasswordKey("pk");
        spec.setExtensions(List.of("seg", "cube"));
        spec.setPasswordSecrets("test-db-1");

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName("test-1").withNamespace(client.getNamespace()).build());
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
                try (Connection con = createConnection(testUser, testPassword, testDatabase)) {
                    log.infof("Create connection to database %s and schema %s", testDatabase, con.getSchema());
                    if (!testSchema.equals(con.getSchema())) {
                        throw new Exception("Wrong connection schema '" + con.getSchema() + "' expected '" + testSchema + "'");
                    }
                    log.infof("Schema created: %s", con.getSchema());
                }
            });
        });

    }

    @Test
    void createUserDatabaseAndCustomSchema() {
        String testUser = "test_user2";
        String testPassword = "test_password2";
        String testDatabase = "test_database2";
        String testSchema = "test_custom2";
        String userSearchPath = "test_custom2,public";

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(testDatabase);
        spec.setUser(testUser);
        spec.setHost(HOST);
        spec.setSchema(testSchema);
        spec.setPasswordKey("pk");
        spec.setUserSearchPath(userSearchPath);
        spec.setPasswordSecrets("test-db-2");

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName("test-2").withNamespace(client.getNamespace()).build());
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
                try (Connection con = createConnection(testUser, testPassword, testDatabase)) {
                    log.infof("Create connection to database %s and schema %s", testDatabase, con.getSchema());
                    if (!testSchema.equals(con.getSchema())) {
                        throw new Exception("Wrong connection schema '" + con.getSchema() + "' expected '" + testSchema + "'");
                    }
                    log.infof("Schema created: %s", con.getSchema());
                }
            });
        });

    }

    @Test
    void createUserDatabaseAndChangePassword() {
        String testUser = "test_user_3";
        String testPassword = "test_password_3";
        String testDatabase = "test_database_3";
        String testSchema = "test_user_3";

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(testDatabase);
        spec.setUser(testUser);
        spec.setHost(HOST);
        spec.setSchema(testSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("test-db-3");

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName("test-3").withNamespace(client.getNamespace()).build());
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
                try (Connection con = createConnection(testUser, testPassword, testDatabase)) {
                    log.infof("Create connection to database %s and schema %s", testDatabase, con.getSchema());
                    if (!testSchema.equals(con.getSchema())) {
                        throw new Exception("Wrong connection schema '" + con.getSchema() + "' expected '" + testSchema + "'");
                    }
                    log.infof("Schema created: %s", con.getSchema());
                }
            });
        });

        String newPassword = "new_user_password";
        log.infof("Update password in secrets %s", newPassword);

        final Secret updatedSecret = client.secrets().inNamespace(client.getNamespace()).withName(spec.getPasswordSecrets())
                .edit(s -> new SecretBuilder(s)
                        .withData(Map.of(spec.getPasswordKey(), encoder.encodeToString(newPassword.getBytes()))).build());
        log.infof("Creating test secret object: %s", updatedSecret);

        log.info("Waiting max 10 seconds for expected database resources to be created and updated");

        await().untilAsserted(() -> {
            Assertions.assertDoesNotThrow(() -> {
                try (Connection con = createConnection(testUser, newPassword, testDatabase)) {
                    log.infof("Create connection to database %s and schema %s", testDatabase, con.getSchema());
                    if (!testSchema.equals(con.getSchema())) {
                        throw new Exception("Wrong connection schema '" + con.getSchema() + "' expected '" + testSchema + "'");
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
}

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

    private static Stream<Arguments> provideDatabaseSpecForTest() {
        return Stream.of(
                Arguments.of("test-1",
                        create("test_database", "test_user", "test_user", "pk", "test-db-1", List.of("seg", "cube"), null),
                        "test_password", "test_user"),
                Arguments.of("test-2",
                        create("test_database2", "test_user2", "test_custom2", "pk2", "test-db-2", null, "test_custom2,public"),
                        "test_password2", "test_custom2"),
                Arguments.of("test-3",
                        create("test_database3", "test_user3", "test_user3", "pk", "test-db-3", null, null),
                        "test_password3", "test_user3"),
                Arguments.of("test-4",
                        create("test_database4", "test_user4", "test_user4", "pk", "test-db-4", List.of(), ""),
                        "test_password4", "test_user4"),
                Arguments.of("test-5",
                        create("test_database5", "test_user5", null, "pk", "test-db-5", null, null),
                        "test_password5", "public"),
                Arguments.of("test-6",
                        create("test_database6", "test_user6", "", "pk", "test-db-6", null, null),
                        "test_password6", "public"));
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

    @Test
    void databaseSpecNulDataTest() {

        String name = "null-data-1";
        String testPassword = "null_data_password";
        String checkSchema = "null_data_schema";
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(name);
        spec.setUser("null_data_1");
        spec.setHost(HOST);
        spec.setSchema(checkSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("null-data-1");
        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client.getNamespace()).build());

        Secret secret = new Secret();
        secret.setMetadata(new ObjectMetaBuilder().withName(spec.getPasswordSecrets())
                .withNamespace(client.getNamespace()).build());
        secret.setData(Map.of(spec.getPasswordKey(), encoder.encodeToString(testPassword.getBytes())));

        log.infof("Creating test database object: %s", database);
        client.resource(database).serverSideApply();

        log.infof("Creating test secret object: %s", secret);
        client.resource(secret).serverSideApply();

        log.info("Waiting 4 seconds and status muss be still null");

        await().pollDelay(4, SECONDS).untilAsserted(() -> {
            DatabaseStatus status = client.resource(database).get().getStatus();
            Assertions.assertNull(status);
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
        String testUser = "change_user_3";
        String testPassword = "change_password_3";
        String testDatabase = "change_database_3";
        String testSchema = "change_user_3";

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(testDatabase);
        spec.setUser(testUser);
        spec.setHost(HOST);
        spec.setSchema(testSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("change-db-3");

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName("change-3").withNamespace(client.getNamespace()).build());
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

    @Test
    void createUserDatabaseAndChangeSpecDataToNull() {
        String name = "null-data-3";
        String testUser = "null_data_user_3";
        String testPassword = "null_data_password_3";
        String testDatabase = "null_data_database_3";
        String testSchema = "null_data_user_3";

        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(testDatabase);
        spec.setUser(testUser);
        spec.setHost(HOST);
        spec.setSchema(testSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("null-data-db-3");

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
                try (Connection con = createConnection(testUser, testPassword, testDatabase)) {
                    log.infof("Create connection to database %s and schema %s", testDatabase, con.getSchema());
                    if (!testSchema.equals(con.getSchema())) {
                        throw new Exception("Wrong connection schema '" + con.getSchema() + "' expected '" + testSchema + "'");
                    }
                    log.infof("Schema created: %s", con.getSchema());
                }
            });
        });

        log.infof("Set spec data to null");

        client.resource(database).inNamespace(client.getNamespace()).edit(d -> {
            d.setSpec(null);
            return d;
        });

    }

    @Test
    void databaseSpecSecretWrongKeyTest() {

        String name = "null-data-2";
        String testPassword = "null_data_password";
        String checkSchema = "null_data_schema";
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(name);
        spec.setUser("null_data_2");
        spec.setHost(HOST);
        spec.setSchema(checkSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("null-data-2");
        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client.getNamespace()).build());
        database.setSpec(spec);

        Secret secret = new Secret();
        secret.setMetadata(new ObjectMetaBuilder().withName(spec.getPasswordSecrets())
                .withNamespace(client.getNamespace()).build());
        secret.setData(Map.of("wrong-key", encoder.encodeToString(testPassword.getBytes())));

        log.infof("Creating test database object: %s", database);
        client.resource(database).serverSideApply();

        log.infof("Creating test secret object: %s", secret);
        client.resource(secret).serverSideApply();

        await().untilAsserted(() -> {
            DatabaseStatus status = client.resource(database).get().getStatus();
            Assertions.assertNotNull(status);
            Assertions.assertEquals("ERROR: Secret key is mandatory. No key secret found!", status.getStatus());
        });

    }

    @Test
    void databaseSpecNullKeyTest() {

        String name = "null-data-3";
        String testPassword = "null_data_password";
        String checkSchema = "null_data_schema";
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(name);
        spec.setUser("null_data_3");
        spec.setHost(HOST);
        spec.setSchema(checkSchema);
        spec.setPasswordSecrets("null-data-3");
        Base64.Encoder encoder = Base64.getEncoder();

        operator.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client.getNamespace()).build());
        database.setSpec(spec);

        Secret secret = new Secret();
        secret.setMetadata(new ObjectMetaBuilder().withName(spec.getPasswordSecrets())
                .withNamespace(client.getNamespace()).build());
        secret.setData(Map.of("wrong-key", encoder.encodeToString(testPassword.getBytes())));

        log.infof("Creating test database object: %s", database);
        client.resource(database).serverSideApply();

        log.infof("Creating test secret object: %s", secret);
        client.resource(secret).serverSideApply();

        await().untilAsserted(() -> {
            DatabaseStatus status = client.resource(database).get().getStatus();
            Assertions.assertNotNull(status);
            Assertions.assertEquals("ERROR: Secret key is mandatory. No key found!", status.getStatus());
        });

    }

    @Test
    void databaseSpecNullKeyValueTest() {

        String name = "null-data-4";
        String checkSchema = "null_data_schema";
        DatabaseSpec spec = new DatabaseSpec();
        spec.setName(name);
        spec.setUser("null_data_4");
        spec.setHost(HOST);
        spec.setSchema(checkSchema);
        spec.setPasswordKey("pk");
        spec.setPasswordSecrets("null-data-4");

        operator.start();

        PostgreSQLDatabase database = new PostgreSQLDatabase();
        database.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client.getNamespace()).build());
        database.setSpec(spec);

        Secret secret = new Secret();
        secret.setMetadata(new ObjectMetaBuilder().withName(spec.getPasswordSecrets())
                .withNamespace(client.getNamespace()).build());
        HashMap<String, String> data = new HashMap<>();
        data.put(spec.getPasswordKey(), null);
        secret.setData(data);

        log.infof("Creating test database object: %s", database);
        client.resource(database).serverSideApply();

        log.infof("Creating test secret object: %s", secret);
        client.resource(secret).serverSideApply();

        await().untilAsserted(() -> {
            DatabaseStatus status = client.resource(database).get().getStatus();
            Assertions.assertNotNull(status);
            Assertions.assertEquals("ERROR: Secret key 'pk' is mandatory. No value found!", status.getStatus());
        });

    }

}

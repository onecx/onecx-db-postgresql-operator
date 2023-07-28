package io.github.onecx.operator.db.postgresql.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.github.onecx.operator.db.postgresql.DatabaseSpec;

@ApplicationScoped
public class DatabaseService {

    private static final Logger log = LoggerFactory.getLogger(DatabaseService.class);

    @Inject
    AgroalDataSource dataSource;

    public void update(String uuid, DatabaseSpec spec, byte[] password) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {

            log.info("[{}] Open database connection.", uuid);

            try (Statement statement = connection.createStatement()) {

                // check user
                boolean userExists = checkUser(uuid, statement, spec);
                log.info("[{}] Check user '{}' if exists '{}'.", uuid, spec.getUser(), userExists);

                // create or update user
                if (userExists) {
                    updateUser(uuid, statement, spec, password);
                    log.info("[{}] Update existing user '{}'", uuid, spec.getUser());
                } else {
                    createUser(uuid, statement, spec, password);
                    log.info("[{}] Create user '{}'", uuid, spec.getUser());
                }

                if (spec.getUserSearchPath() != null && !spec.getUserSearchPath().isBlank()) {
                    updateUserSearchPath(uuid, statement, spec);
                    log.info("[{}] Update user '{}' search path to '{}'", uuid, spec.getUser(),
                            spec.getUserSearchPath());
                }

                // check database
                boolean dbExists = checkDatabase(uuid, statement, spec);
                log.info("[{}] Check database '{}' if exists '{}'", uuid, spec.getName(), dbExists);

                // create or update database
                if (dbExists) {
                    updateDatabase(uuid, statement, spec);
                    log.info("[{}] Update database '{}'", uuid, spec.getName());
                } else {
                    createDatabase(uuid, statement, spec);
                    log.info("[{}] Create database '{}'", uuid, spec.getName());
                }

            }
        } finally {
            log.info("[{}] Close database connection.", uuid);
        }

        try (AgroalDataSource datasource = createDatasource(spec.getName())) {
            try (Connection connection = datasource.getConnection()) {

                log.info("[{}] Open database '{}' connection.", uuid, spec.getName());

                try (Statement statement = connection.createStatement()) {
                    // create schema if not exists
                    if (spec.getSchema() != null && !spec.getSchema().isBlank()) {
                        createSchema(uuid, statement, spec);
                        log.info("[{}] Create schema '{}'", uuid, spec.getSchema());
                    }

                    // create extension if not exists
                    if (spec.getExtensions() != null && !spec.getExtensions().isEmpty()) {
                        createExtensions(uuid, statement, spec);
                        log.info("[{}] Create extensions '{}'", uuid, spec.getExtensions());
                    }
                }
            }
        } finally {
            log.info("[{}] Close database '{}' connection.", uuid, spec.getName());
        }
    }

    private boolean checkDatabase(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            return statement
                    .executeQuery(
                            String.format("SELECT true FROM pg_catalog.pg_database WHERE datname = '%s'",
                                    spec.getName()))
                    .next();
        } catch (SQLException e) {
            log.error("[{}] Failed to check database '{}'", uuid, spec.getName());
            throw e;
        }
    }

    private void createSchema(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            statement.execute(
                    String.format("CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s;", spec.getSchema(),
                            spec.getUser()));
        } catch (SQLException e) {
            log.error("[{}] Failed to create schema '{}' for user '{}'", uuid, spec.getSchema(),
                    spec.getUser());
            throw e;
        }
    }

    private void updateUserSearchPath(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            statement.execute(
                    String.format("ALTER USER %s SET SEARCH_PATH TO %s;", spec.getUser(),
                            spec.getUserSearchPath()));
        } catch (SQLException e) {
            log.error("[{}] Failed to update user '{}' search path '{}'", uuid, spec.getUser(),
                    spec.getUserSearchPath());
            throw e;
        }
    }

    private void updateDatabase(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            statement.execute(String.format("ALTER DATABASE %s OWNER TO %s", spec.getName(), spec.getUser()));
        } catch (SQLException e) {
            log.error("[{}] Failed to update database: '{}'", uuid, spec.getName());
            throw e;
        }
    }

    private void createDatabase(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            statement.execute(String.format("CREATE DATABASE %s OWNER '%s'", spec.getName(), spec.getUser()));
        } catch (SQLException e) {
            log.error("[{}] Failed to create database: '{}'", uuid, spec.getName());
            throw e;
        }
    }

    private boolean checkUser(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        try {
            return statement
                    .executeQuery(String.format("SELECT true FROM pg_user WHERE usename = '%s'", spec.getUser()))
                    .next();
        } catch (SQLException e) {
            log.error("[{}] Failed to check user: '{}'", uuid, spec.getUser());
            throw e;
        }
    }

    private void updateUser(String uuid, Statement statement, DatabaseSpec spec, byte[] password) throws SQLException {
        try {
            statement.execute(String.format("ALTER USER %s PASSWORD '%s'", spec.getUser(), new String(password)));
        } catch (SQLException e) {
            log.error("[{}] Failed to update user: '{}'", uuid, spec.getUser());
            throw e;
        }
    }

    private void createUser(String uuid, Statement statement, DatabaseSpec spec, byte[] password) throws SQLException {
        try {
            statement
                    .execute(
                            String.format("CREATE USER %s WITH ENCRYPTED PASSWORD '%s'", spec.getUser(), new String(password)));
        } catch (SQLException e) {
            log.error("[{}] Failed to create user: '{}'", uuid, spec.getUser());
            throw e;
        }
    }

    private void createExtensions(String uuid, Statement statement, DatabaseSpec spec) throws SQLException {
        if (spec.getExtensions().isEmpty()) {
            return;
        }
        for (String extension : spec.getExtensions()) {
            try {
                statement.execute(String.format("CREATE EXTENSION IF NOT EXISTS \"%s\"", extension));
            } catch (SQLException e) {
                log.error("[{}] Failed to create extension: '{}'", uuid, extension);
                throw e;
            }
        }
    }

    private AgroalDataSource createDatasource(String database) throws SQLException {

        AgroalDataSourceConfigurationSupplier dataSourceConfiguration = new AgroalDataSourceConfigurationSupplier();

        dataSourceConfiguration.connectionPoolConfiguration(dataSource.getConfiguration().connectionPoolConfiguration());

        String jdbcUrl = dataSource.getConfiguration().connectionPoolConfiguration().connectionFactoryConfiguration().jdbcUrl();
        jdbcUrl = createJdbcUrl(jdbcUrl, database);

        AgroalConnectionPoolConfigurationSupplier poolConfiguration = dataSourceConfiguration.connectionPoolConfiguration();
        AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration = poolConfiguration
                .connectionFactoryConfiguration();

        connectionFactoryConfiguration.jdbcUrl(jdbcUrl);
        return AgroalDataSource.from(dataSourceConfiguration.get());
    }

    static String createJdbcUrl(String jdbcUrl, String database) {
        int startIndex = jdbcUrl.lastIndexOf("/");
        int endIndex = jdbcUrl.lastIndexOf("?");
        String result = jdbcUrl.substring(0, startIndex + 1) + database;
        if (endIndex > -1) {
            result = result + jdbcUrl.substring(endIndex);
        }
        return result;
    }

}

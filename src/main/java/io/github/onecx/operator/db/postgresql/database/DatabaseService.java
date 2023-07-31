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

/**
 * Database service to access database and execute changes.
 */
@ApplicationScoped
public class DatabaseService {

    private static final Logger log = LoggerFactory.getLogger(DatabaseService.class);

    private static final String SQL_CHECK_USER = "SELECT true FROM pg_user WHERE usename = '%s'";
    private static final String SQL_UPDATE_USER = "ALTER USER %s PASSWORD '%s'";
    private static final String SQL_CREATE_USER = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s'";
    private static final String SQL_USER_SEARCH_PATH = "ALTER USER %s SET SEARCH_PATH TO %s;";
    private static final String SQL_USER_EXTENSION = "CREATE EXTENSION IF NOT EXISTS \"%s\"";
    private static final String SQL_CHECK_DB = "SELECT true FROM pg_catalog.pg_database WHERE datname = '%s'";
    private static final String SQL_UPDATE_DB = "ALTER DATABASE %s OWNER TO %s";
    private static final String SQL_CREATE_DB = "CREATE DATABASE %s OWNER '%s'";
    private static final String SQL_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s;";

    @Inject
    AgroalDataSource dataSource;

    public void update(String uuid, DatabaseSpec spec, byte[] password) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {

            log.info("[{}] Open database connection.", uuid);

            try (Statement statement = connection.createStatement()) {

                // check user
                boolean userExists = statement.executeQuery(String.format(SQL_CHECK_USER, spec.getUser())).next();
                log.info("[{}] Check user '{}' if exists '{}'.", uuid, spec.getUser(), userExists);

                // create or update user
                if (userExists) {
                    statement.execute(String.format(SQL_UPDATE_USER, spec.getUser(), new String(password)));
                    log.info("[{}] Update existing user '{}'", uuid, spec.getUser());
                } else {
                    statement.execute(String.format(SQL_CREATE_USER, spec.getUser(), new String(password)));
                    log.info("[{}] Create user '{}'", uuid, spec.getUser());
                }

                if (spec.getUserSearchPath() != null && !spec.getUserSearchPath().isBlank()) {
                    statement.execute(String.format(SQL_USER_SEARCH_PATH, spec.getUser(), spec.getUserSearchPath()));
                    log.info("[{}] Update user '{}' search path to '{}'", uuid, spec.getUser(),
                            spec.getUserSearchPath());
                }

                // check database
                boolean dbExists = statement.executeQuery(String.format(SQL_CHECK_DB, spec.getName())).next();
                log.info("[{}] Check database '{}' if exists '{}'", uuid, spec.getName(), dbExists);

                // create or update database
                if (dbExists) {
                    statement.execute(String.format(SQL_UPDATE_DB, spec.getName(), spec.getUser()));
                    log.info("[{}] Update database '{}'", uuid, spec.getName());
                } else {
                    statement.execute(String.format(SQL_CREATE_DB, spec.getName(), spec.getUser()));
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
                        statement.execute(String.format(SQL_CREATE_SCHEMA, spec.getSchema(), spec.getUser()));
                        log.info("[{}] Create schema '{}'", uuid, spec.getSchema());
                    }

                    // create extension if not exists
                    if (spec.getExtensions() != null && !spec.getExtensions().isEmpty()) {
                        for (String extension : spec.getExtensions()) {
                            statement.execute(String.format(SQL_USER_EXTENSION, extension));
                        }
                        log.info("[{}] Create extensions '{}'", uuid, spec.getExtensions());
                    }
                }
            }
        } finally {
            log.info("[{}] Close database '{}' connection.", uuid, spec.getName());
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

package io.github.onecx.operator.db.postgresql.database;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class DatabaseServiceTest {

    @ParameterizedTest
    @MethodSource("createJdbcUrlParameters")
    void createJdbcUrlTest(String url, String database, String result) {
        String tmp = DatabaseService.createJdbcUrl(url, database);
        Assertions.assertEquals(result, tmp);
    }

    private static Stream<Arguments> createJdbcUrlParameters() {
        return Stream.of(
                Arguments.of("jdbc:postgresql://localhost:32769/quarkus", "12345", "jdbc:postgresql://localhost:32769/12345"),
                Arguments.of("jdbc:postgresql://localhost:32769/quarkus?loggerLevel=OFF", "12345",
                        "jdbc:postgresql://localhost:32769/12345?loggerLevel=OFF"),
                Arguments.of("jdbc:postgresql://localhost:32769/quarkus", "12345",
                        "jdbc:postgresql://localhost:32769/12345"));
    }

}

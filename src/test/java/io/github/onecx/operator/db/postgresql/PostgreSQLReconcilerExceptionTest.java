package io.github.onecx.operator.db.postgresql;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class PostgreSQLReconcilerExceptionTest {

    @Inject
    PostgreSQLReconciler reconciler;

    @Test
    void updateErrorStatusCustomExceptionTest() {
        PostgreSQLDatabase pd = new PostgreSQLDatabase();
        reconciler.updateErrorStatus(pd, null, new RuntimeException("Custom error"));

        Assertions.assertNotNull(pd.getStatus());
        DatabaseStatus status = pd.getStatus();
        Assertions.assertEquals("ERROR: Custom error", status.getStatus());
    }

}

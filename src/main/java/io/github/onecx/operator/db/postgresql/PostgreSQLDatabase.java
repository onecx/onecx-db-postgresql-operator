package io.github.onecx.operator.db.postgresql;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("io.github.onecx.operator.db.postgresql")
@Version("v1")
public class PostgreSQLDatabase extends CustomResource<DatabaseSpec, DatabaseStatus> implements Namespaced {
}
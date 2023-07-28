package io.github.onecx.operator.db.postgresql;

import java.util.*;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Secret;
import io.github.onecx.operator.db.postgresql.database.DatabaseService;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.filter.OnAddFilter;
import io.javaoperatorsdk.operator.processing.event.source.filter.OnUpdateFilter;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

@ControllerConfiguration(onAddFilter = PostgreSQLReconciler.SecretAddFilter.class, onUpdateFilter = PostgreSQLReconciler.SecretUpdateFilter.class)
public class PostgreSQLReconciler implements Reconciler<PostgreSQLDatabase>, ErrorStatusHandler<PostgreSQLDatabase>,
        EventSourceInitializer<PostgreSQLDatabase> {

    private static final Logger log = LoggerFactory.getLogger(PostgreSQLReconciler.class);

    static final String HOST = ConfigProvider.getConfig().getValue("onecx.operator.db.postgresql.host", String.class);

    @Inject
    DatabaseService databaseService;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<PostgreSQLDatabase> context) {
        final SecondaryToPrimaryMapper<Secret> webappsMatchingTomcatName = (Secret t) -> context.getPrimaryCache()
                .list(db -> db.getSpec().getPasswordSecrets().equals(t.getMetadata().getName()))
                .map(ResourceID::fromResource)
                .collect(Collectors.toSet());

        InformerConfiguration<Secret> configuration = InformerConfiguration.from(Secret.class, context)
                .withSecondaryToPrimaryMapper(webappsMatchingTomcatName)
                .withPrimaryToSecondaryMapper(
                        (PostgreSQLDatabase primary) -> Set.of(new ResourceID(primary.getSpec().getPasswordSecrets(),
                                primary.getMetadata().getNamespace())))
                .build();
        return EventSourceInitializer
                .nameEventSources(new InformerEventSource<>(configuration, context));
    }

    @Override
    public UpdateControl<PostgreSQLDatabase> reconcile(PostgreSQLDatabase database, Context<PostgreSQLDatabase> context)
            throws Exception {

        Optional<Secret> secret = context.getSecondaryResource(Secret.class);
        if (secret.isPresent()) {

            String name = database.getMetadata().getName();
            String namespace = database.getMetadata().getNamespace();
            String uuid = UUID.randomUUID().toString();

            log.info("[{}] Reconcile postgresql database: {} namespace: {}", uuid, name, namespace);
            try {
                byte[] password = createRequestData(database.getSpec(), secret.get());
                databaseService.update(uuid, database.getSpec(), password);
            } catch (Exception te) {
                throw new ReconcileException(uuid, te);
            }

            updateStatusPojo(database);
            log.info("Database '{}' reconciled - updating status", database.getMetadata().getName());
            return UpdateControl.updateStatus(database);
        }
        return UpdateControl.noUpdate();
    }

    private static byte[] createRequestData(DatabaseSpec spec, Secret secret) throws MissingMandatoryKeyException {
        Map<String, String> data = secret.getData();
        if (data == null || data.isEmpty()) {
            throw new MissingMandatoryKeyException("Secret is empty!");
        }
        String key = spec.getPasswordKey();
        if (key == null || !data.containsKey(key)) {
            throw new MissingMandatoryKeyException("Secret key is mandatory. No key found!");
        }
        String value = data.get(key);
        if ((value == null || value.isEmpty())) {
            throw new MissingMandatoryKeyException("Secret key '{}' is mandatory. No value found!");
        }
        return Base64.getDecoder().decode(value);
    }

    public static class MissingMandatoryKeyException extends Exception {

        public MissingMandatoryKeyException(String msg) {
            super(msg);
        }
    }

    public static class ReconcileException extends Exception {

        final String uuid;

        ReconcileException(String uuid, Exception ex) {
            super("Error reconcile resource", ex);
            this.uuid = uuid;
        }

    }

    @Override
    public ErrorStatusUpdateControl<PostgreSQLDatabase> updateErrorStatus(PostgreSQLDatabase resource,
            Context<PostgreSQLDatabase> context, Exception e) {
        String uuid = null;
        if (e instanceof ReconcileException re) {
            uuid = re.uuid;
        }

        log.error("[{}] Error reconcile resource", uuid, e);
        DatabaseStatus status = new DatabaseStatus();
        status.setUrl(null);
        status.setUser(null);
        status.setPasswordSecrets(null);
        status.setStatus("ERROR: " + e.getMessage());
        resource.setStatus(status);
        return ErrorStatusUpdateControl.updateStatus(resource);
    }

    private void updateStatusPojo(PostgreSQLDatabase database) {
        DatabaseStatus status = new DatabaseStatus();
        DatabaseSpec spec = database.getSpec();
        status.setUrl(spec.getName());
        status.setUser(spec.getUser());
        status.setPasswordSecrets(spec.getPasswordSecrets());
        status.setStatus("CREATED");
        database.setStatus(status);
    }

    public static class SecretAddFilter implements OnAddFilter<PostgreSQLDatabase> {

        @Override
        public boolean accept(PostgreSQLDatabase resource) {
            if (HOST == null) {
                return false;
            }
            return HOST.equals(resource.getSpec().getHost());
        }
    }

    public static class SecretUpdateFilter implements OnUpdateFilter<PostgreSQLDatabase> {

        @Override
        public boolean accept(PostgreSQLDatabase newResource, PostgreSQLDatabase oldResource) {
            if (oldResource.getMetadata().getResourceVersion().equals(newResource.getMetadata().getResourceVersion())) {
                return false;
            }
            if (HOST == null) {
                return false;
            }
            return HOST.equals(newResource.getSpec().getHost());
        }
    }
}

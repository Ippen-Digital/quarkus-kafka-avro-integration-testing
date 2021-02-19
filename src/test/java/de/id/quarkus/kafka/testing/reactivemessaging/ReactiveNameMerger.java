package de.id.quarkus.kafka.testing.reactivemessaging;

import de.id.avro.AccountTransaction;
import de.id.avro.SimpleName;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;

@ApplicationScoped
public class ReactiveNameMerger {

    @Incoming("source-topic")
    @Outgoing("target-topic")
    public Multi<Record<String, SimpleName>> transformNames(AccountTransaction sourceEvent) {
        return Multi.createFrom().item(sourceEvent)
                .map(sourceTopicEvent -> Record.of(Instant.now().toString(), toSimpleName(sourceEvent)));
    }

    private SimpleName toSimpleName(AccountTransaction sourceEvent) {
        return new SimpleName(String.format("%s %s", sourceEvent.getPrename(), sourceEvent.getSurname()));
    }
}

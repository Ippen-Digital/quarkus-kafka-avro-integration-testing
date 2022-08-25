package de.id.quarkus.kafka.testing.reactivemessaging_multibootstrap;

import de.id.avro.Donation;
import de.id.avro.Donator;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;

@ApplicationScoped
public class MultiBootstrapDonatorExtractor {

    @Incoming("mb-source")
    @Outgoing("mb-target")
    public Multi<Record<String, Donator>> transformNames(Donation sourceEvent) {
        return Multi.createFrom().item(sourceEvent)
                .map(sourceTopicEvent -> Record.of(Instant.now().toString(), toSimpleName(sourceEvent)));
    }

    private Donator toSimpleName(Donation sourceEvent) {
        return new Donator(String.format("%s %s", sourceEvent.getPrename(), sourceEvent.getSurname()), null);
    }
}

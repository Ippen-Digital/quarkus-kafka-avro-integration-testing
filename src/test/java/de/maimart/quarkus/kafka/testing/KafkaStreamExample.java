package de.maimart.quarkus.kafka.testing;

import de.maimart.avro.SourceTopicEvent;
import de.maimart.avro.TargetTopicEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;

@ApplicationScoped
public class KafkaStreamExample {


    @Incoming("source-topic")
    @Outgoing("target-topic")
    public Multi<Record<String, TargetTopicEvent>> transformNames(SourceTopicEvent sourceEvent) {
        return Multi.createFrom().item(sourceEvent)
                .map(sourceTopicEvent -> Record.of(Instant.now().toString(), toTargetTopicEvent(sourceEvent)));
    }

    private TargetTopicEvent toTargetTopicEvent(SourceTopicEvent sourceEvent) {
        return new TargetTopicEvent(String.format("%s %s", sourceEvent.getPrename(), sourceEvent.getSurname()));
    }
}

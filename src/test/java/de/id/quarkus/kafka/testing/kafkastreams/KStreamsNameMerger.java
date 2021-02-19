package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.AccountTransaction;
import de.id.avro.SimpleName;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class KStreamsNameMerger {

    private static final String SOURCE_TOPIC = "kafkastreams.source-topic";
    private static final String TARGET_TOPIC = "kafkastreams.target-topic";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, AccountTransaction> stream = streamsBuilder.stream(SOURCE_TOPIC);
        stream.mapValues(this::toSimpleName)
                .to(TARGET_TOPIC);
        return streamsBuilder.build();
    }

    private SimpleName toSimpleName(AccountTransaction sourceEvent) {
        return new SimpleName(String.format("%s %s", sourceEvent.getPrename(), sourceEvent.getSurname()));
    }
}

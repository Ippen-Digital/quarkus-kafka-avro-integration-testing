package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.Donation;
import de.id.avro.Donator;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class KStreamsDonatorExtractor {

    private static final String SOURCE_TOPIC = "kafkastreams.source-topic";
    private static final String TARGET_TOPIC = "kafkastreams.target-topic";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Donation> stream = streamsBuilder.stream(SOURCE_TOPIC);
        stream.mapValues(this::toSimpleName)
                .to(TARGET_TOPIC);
        return streamsBuilder.build();
    }

    private Donator toSimpleName(Donation sourceEvent) {
        return new Donator(String.format("%s %s", sourceEvent.getPrename(), sourceEvent.getSurname()));
    }
}

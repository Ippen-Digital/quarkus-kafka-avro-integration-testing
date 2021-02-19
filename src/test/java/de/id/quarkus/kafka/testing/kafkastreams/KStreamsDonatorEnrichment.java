package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.Donation;
import de.id.avro.DonationCollector;
import de.id.avro.Donator;
import de.id.quarkus.kafka.testing.scenarios.DonatorEnrichmentProfile;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.quarkus.arc.profile.IfBuildProfile;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.HashMap;

@ApplicationScoped
public class KStreamsDonatorEnrichment {

    public static final String DONATION_TOPIC = "kafkastreams.donation";
    public static final String DONATION_COLLECTOR_TOPIC = "kafkastreams.collector";
    public static final String DONATOR_TOPIC = "kafkastreams.donator";

    @ConfigProperty(name = "quarkus.kafka-streams.schema-registry-url")
    String schemaRegistryUrl;

    @Produces
    @IfBuildProfile(DonatorEnrichmentProfile.PROFILE_NAME)
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<Integer, DonationCollector> collectors = builder.globalTable(
                DONATION_COLLECTOR_TOPIC, Consumed.with(Serdes.Integer(), buildAvroValueSerde(DonationCollector.class)));

        builder.stream(DONATION_TOPIC, Consumed.with(Serdes.Integer(), buildAvroValueSerde(Donation.class)))
                .join(
                        collectors,
                        (key, donation) -> donation.getDonationCollectorId(),
                        this::toDonator
                )
                .to(DONATOR_TOPIC, Produced.with(Serdes.Integer(), buildAvroValueSerde(Donator.class)));
        return builder.build();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> buildAvroValueSerde(Class<T> clazz) {
        HashMap<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, false);
        return serde;
    }

    private Donator toDonator(Donation donation, DonationCollector donationCollector) {
        return new Donator(String.format("%s %s", donation.getPrename(), donation.getSurname()), donationCollector.getProjectName());
    }
}

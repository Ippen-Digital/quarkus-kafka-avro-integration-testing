package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.Donation;
import de.id.avro.Donator;
import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(value = ConfluentStack.class)
class KStreamsDonatorExtractorTest {

    public static final int MAX_CONSUMER_WAIT_TIME = 10000;
    private static final String SOURCE_TOPIC = "kafkastreams.source-topic";
    private static final String TARGET_TOPIC = "kafkastreams.target-topic";

    ConfluentStackClient testClusterClient;

    @BeforeEach
    void setUp() {
        testClusterClient.createTopics(SOURCE_TOPIC, TARGET_TOPIC);
    }

    @Test
    void shouldExtractADonatorOutOfEveryDonation() throws InterruptedException, ExecutionException, TimeoutException {
        Donation donation = Donation.newBuilder().setPrename("Max").setSurname("Mustermann").build();
        List<Donation> donationsToSend = IntStream.range(0, 10).mapToObj(i -> donation).collect(Collectors.toList());

        Future<List<Donator>> receiveFuture = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", donationsToSend.size(), StringDeserializer.class);

        testClusterClient.sendRecords(SOURCE_TOPIC, donationsToSend, StringSerializer.class, (index, event) -> String.valueOf(index));

        List<Donator> receivedDonators = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedDonators).hasSameSizeAs(donationsToSend);
    }

    @Test
    void shouldExtractDonatorsName() throws InterruptedException, ExecutionException, TimeoutException {
        Donation donation = Donation.newBuilder().setPrename("Max").setSurname("Mustermann").build();

        Future<List<Donator>> receiveFuture = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 1, StringDeserializer.class);

        testClusterClient.sendRecords(SOURCE_TOPIC, Collections.singletonList(donation), StringSerializer.class, (index, event) -> String.valueOf(index));

        List<Donator> receivedDonators = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedDonators).hasSize(1);
        assertThat(receivedDonators.get(0).getName()).asString().isEqualTo("Max Mustermann");
    }
}
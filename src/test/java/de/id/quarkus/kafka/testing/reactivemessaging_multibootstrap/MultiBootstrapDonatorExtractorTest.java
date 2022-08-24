package de.id.quarkus.kafka.testing.reactivemessaging_multibootstrap;

import de.id.avro.Donation;
import de.id.avro.Donator;
import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import de.id.quarkus.kafka.testing.scenarios.DonatorExtractorProfile;
import de.id.quarkus.kafka.testing.scenarios.MultiBootstrapDonatorExtractorProfile;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
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
@QuarkusTestResource(value = ConfluentStack.class, initArgs = {
        @ResourceArg(name = "incoming", value = "source-topic"),
        @ResourceArg(name = "incomingTopic", value = "reactivemessaging.source-topic"),
        @ResourceArg(name = "outgoing", value = "target-topic"),
        @ResourceArg(name = "outgoingTopic", value = "reactivemessaging.target-topic")
})
class MultiBootstrapDonatorExtractorTest {

    public static final int MAX_CONSUMER_WAIT_TIME = 5000;
    private static final String SOURCE_TOPIC = "reactivemessaging.source-topic";
    private static final String TARGET_TOPIC = "reactivemessaging.target-topic";

    ConfluentStackClient testClusterClient;

    @BeforeEach
    void setUp() {
        testClusterClient.createTopics(SOURCE_TOPIC, TARGET_TOPIC);
    }

    @Test
    void shouldExtractDonatorsName() throws InterruptedException, ExecutionException, TimeoutException {
        Donation donation = new Donation("Max", "Mustermann", 10.0, 111);

        Future<List<Donator>> receiveFuture = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 1, StringDeserializer.class);

        testClusterClient.sendRecords(SOURCE_TOPIC, Collections.singletonList(donation), StringSerializer.class, (index, event) -> String.valueOf(index));

        List<Donator> receivedDonators = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedDonators).hasSize(1);
        assertThat(receivedDonators.get(0).getName()).asString().isEqualTo("Max Mustermann");
    }
}
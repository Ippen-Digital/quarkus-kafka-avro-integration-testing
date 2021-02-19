package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.AccountTransaction;
import de.id.avro.SimpleName;
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
class KStreamsNameMergerTest {

    public static final int MAX_CONSUMER_WAIT_TIME = 10000;
    private static final String SOURCE_TOPIC = "kafkastreams.source-topic";
    private static final String TARGET_TOPIC = "kafkastreams.target-topic";

    ConfluentStackClient testClusterClient;

    @BeforeEach
    void setUp() {
        testClusterClient.createTopics(SOURCE_TOPIC, TARGET_TOPIC);
    }

    @Test
    void shouldEmmitAllEvents() throws InterruptedException, ExecutionException, TimeoutException {
        AccountTransaction accountTransaction = AccountTransaction.newBuilder().setPrename("Max").setSurname("Mustermann").build();
        List<AccountTransaction> eventsToSend = IntStream.range(0, 10).mapToObj(i -> accountTransaction).collect(Collectors.toList());

        Future<List<SimpleName>> receiveFuture = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", eventsToSend.size(), StringDeserializer.class);

        testClusterClient.sendRecords(SOURCE_TOPIC, eventsToSend, StringSerializer.class, (index, event) -> String.valueOf(index));

        List<SimpleName> receivedNames = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedNames).hasSameSizeAs(eventsToSend);
    }

    @Test
    void shouldEmmitCorrectlyTransformedEvents() throws InterruptedException, ExecutionException, TimeoutException {
        AccountTransaction accountTransaction = AccountTransaction.newBuilder().setPrename("Max").setSurname("Mustermann").build();

        Future<List<SimpleName>> receiveFuture = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 1, StringDeserializer.class);

        testClusterClient.sendRecords(SOURCE_TOPIC, Collections.singletonList(accountTransaction), StringSerializer.class, (index, event) -> String.valueOf(index));

        List<SimpleName> receivedNames = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedNames).hasSize(1);
        assertThat(receivedNames.get(0).getName()).asString().isEqualTo("Max Mustermann");
    }
}
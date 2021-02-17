package de.id.quarkus.kafka.testing.reactivemessaging;

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(value = ConfluentStack.class)
class NameMergerTest {

    private static final String SOURCE_TOPIC = "de.id.source-topic";
    private static final String TARGET_TOPIC = "de.id.target-topic";

    ConfluentStackClient testClusterClient;

    @BeforeEach
    void setUp() throws InterruptedException {
        testClusterClient.deleteAllTopics();
        testClusterClient.deleteAllConsumerGroups();
        // wait until deletion
        Thread.sleep(1000);
        testClusterClient.createTopics(SOURCE_TOPIC, TARGET_TOPIC);
        testClusterClient.registerSchemaRegistryTypes(AccountTransaction.getClassSchema());
        testClusterClient.registerSchemaRegistryTypes(SimpleName.getClassSchema());
    }

    @Test
    void shouldEmmitAllEvents() {
        AccountTransaction accountTransaction = AccountTransaction.newBuilder().setPrename("Max").setSurname("Mustermann").build();
        List<AccountTransaction> eventsToSend = IntStream.range(0, 10).mapToObj(i -> accountTransaction).collect(Collectors.toList());

        testClusterClient.sendRecords(SOURCE_TOPIC, eventsToSend, StringSerializer.class, (index, event) -> String.valueOf(index));

        List<SimpleName> receivedNames = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 10000, eventsToSend.size(), StringDeserializer.class);

        assertThat(receivedNames).hasSameSizeAs(eventsToSend);
    }

    @Test
    void shouldEmmitCorrectlyTransformedEvents() {
        AccountTransaction accountTransaction = AccountTransaction.newBuilder().setPrename("Max").setSurname("Mustermann").build();

        testClusterClient.sendRecords(SOURCE_TOPIC, Collections.singletonList(accountTransaction), StringSerializer.class, (index, event) -> String.valueOf(index));

        List<SimpleName> receivedNames = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 10000, 1, StringDeserializer.class);

        SimpleName receivedEvent = receivedNames.get(0);

        assertThat(receivedEvent.getName()).asString().isEqualTo("Max Mustermann");
    }
}
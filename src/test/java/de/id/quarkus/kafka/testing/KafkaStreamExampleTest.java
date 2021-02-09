package de.id.quarkus.kafka.testing;

import de.id.avro.SourceTopicEvent;
import de.id.avro.TargetTopicEvent;
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
class KafkaStreamExampleTest {

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
        testClusterClient.registerSchemaRegistryTypes(SourceTopicEvent.getClassSchema());
        testClusterClient.registerSchemaRegistryTypes(TargetTopicEvent.getClassSchema());
    }

    @Test
    void shouldEmmitAllEvents() {
        SourceTopicEvent sourceTopicEvent = new SourceTopicEvent("Max", "Mustermann");
        List<SourceTopicEvent> eventsToSend = IntStream.range(0, 10).mapToObj(i -> sourceTopicEvent).collect(Collectors.toList());

        testClusterClient.sendRecords(SOURCE_TOPIC, eventsToSend, StringSerializer.class, (index, event) -> String.valueOf(index));

        List<TargetTopicEvent> receivedEvents = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 10000, eventsToSend.size(), StringDeserializer.class);

        assertThat(receivedEvents).hasSameSizeAs(eventsToSend);
    }

    @Test
    void shouldEmmitCorrectlyTransformedEvents() {
        SourceTopicEvent sourceTopicEvent = new SourceTopicEvent("Max", "Mustermann");

        testClusterClient.sendRecords(SOURCE_TOPIC, Collections.singletonList(sourceTopicEvent), StringSerializer.class, (index, event) -> String.valueOf(index));

        List<TargetTopicEvent> receivedEvents = testClusterClient.waitForRecords(TARGET_TOPIC, "testConsumerGroup", 10000, 1, StringDeserializer.class);

        TargetTopicEvent receivedEvent = receivedEvents.get(0);

        assertThat(receivedEvent.getName()).asString().isEqualTo("Max Mustermann");
    }
}
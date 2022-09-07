package de.id.quarkus.kafka.testing.kafkastreams;

import de.id.avro.Donation;
import de.id.avro.DonationCollector;
import de.id.avro.Donator;
import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import de.id.quarkus.kafka.testing.scenarios.DonatorEnrichmentProfile;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.id.quarkus.kafka.testing.kafkastreams.KStreamsDonatorEnrichment.*;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(DonatorEnrichmentProfile.class)
@QuarkusTestResource(value = ConfluentStack.class, restrictToAnnotatedClass = true)
class KStreamsDonatorEnrichmentTest {

    public static final int MAX_CONSUMER_WAIT_TIME = 5000;

    ConfluentStackClient testClusterClient;

    @BeforeEach
    void setUp() {
        testClusterClient.createTopics(DONATOR_TOPIC, DONATION_TOPIC);
        HashMap<String, String> collectorsTopicConfigs = new HashMap<>();
        collectorsTopicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        NewTopic collectorsTopic = new NewTopic(DONATION_COLLECTOR_TOPIC, 1, (short) 1)
                .configs(collectorsTopicConfigs);
        testClusterClient.createAdminClient().createTopics(Collections.singletonList(collectorsTopic));
    }

    @Test
    void shouldExtractADonatorContainingNameOfDonationProject() throws InterruptedException, ExecutionException, TimeoutException {
        List<DonationCollector> donationCollectors = donationCollectors();
        testClusterClient.sendRecordsToTopic(DONATION_COLLECTOR_TOPIC, donationCollectors, IntegerSerializer.class, (integer, donationCollector) -> donationCollector.getId());

        Future<List<Donator>> receive = testClusterClient.waitForRecordsFromTopic(DONATOR_TOPIC, "donators", 1, IntegerDeserializer.class);

        Donation donation = new Donation("Max", "Mustermann", 10.0, 222);
        testClusterClient.sendRecordsToTopic(DONATION_TOPIC, Collections.singletonList(donation), IntegerSerializer.class, (index, don) -> index);

        List<Donator> donators = receive.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(donators).hasSize(1);
        assertThat(donators.get(0).getNameOfDonationProject()).isEqualTo("Project 222");
    }

    @Test
    void shouldAddTheMoneyOnTheDonationCollector() throws InterruptedException, ExecutionException, TimeoutException {
        List<DonationCollector> donationCollectors = donationCollectors();
        testClusterClient.sendRecordsToTopic(DONATION_COLLECTOR_TOPIC, donationCollectors, IntegerSerializer.class, (integer, donationCollector) -> donationCollector.getId());

        Future<List<DonationCollector>> receiveCollectors = testClusterClient.waitForRecordsFromTopic(DONATION_COLLECTOR_TOPIC, "donator-collections", 1, IntegerDeserializer.class);

        Donation donation = new Donation("Max", "Mustermann", 10.0, 222);
        testClusterClient.sendRecordsToTopic(DONATION_TOPIC, Collections.singletonList(donation), IntegerSerializer.class, (index, don) -> index);

        List<DonationCollector> updatedDonationCollectors = receiveCollectors.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(updatedDonationCollectors).hasSize(1);

        DonationCollector updatedDonationCollector = updatedDonationCollectors.get(0);
        assertThat(updatedDonationCollector.getId()).isEqualTo(222);
        assertThat(updatedDonationCollector.getBalance()).isEqualTo(donation.getAmount());
    }

    private List<DonationCollector> donationCollectors() {
        List<DonationCollector> list = new ArrayList<>();
        list.add(new DonationCollector(111, "Project 111", 0D));
        list.add(new DonationCollector(222, "Project 222", 0D));
        list.add(new DonationCollector(333, "Project 333", 0D));
        return list;

    }
}
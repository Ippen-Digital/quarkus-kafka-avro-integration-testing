package de.id.quarkus.kafka.testing.reactivemessaging_multibootstrap;

import de.id.avro.Donation;
import de.id.avro.Donator;
import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
@QuarkusTestResource(
        value = ConfluentStack.class,
        initArgs = {
                @ResourceArg(name = "incoming", value = "mb-source"),
                @ResourceArg(name = "outgoing", value = "mb-target")
        },
        // we need this to avoid unpredictable tests configuration due to double starts of ConfluenceStack:
        // https://github.com/quarkusio/quarkus/issues/22025#:~:text=apply%20this%20argument%20to%20all%20tests
        restrictToAnnotatedClass = true)
class MultiBootstrapDonatorExtractorTest {

    public static final int MAX_CONSUMER_WAIT_TIME = 5000;

    ConfluentStackClient testClusterClient;

    @Test
    void shouldExtractADonatorOutOfEveryDonation() throws InterruptedException, ExecutionException, TimeoutException {
        Donation donation = new Donation("Foo", "Bar", 10.0, 111);
        List<Donation> donationToSend = IntStream.range(0, 10).mapToObj(i -> donation).collect(Collectors.toList());

        Future<List<Donator>> receiveFuture = testClusterClient.waitForRecords("testConsumerGroup",
                donationToSend.size(), StringDeserializer.class);

        testClusterClient.sendRecords(donationToSend, StringSerializer.class,
                (index, event) -> String.valueOf(index));

        List<Donator> receivedDonators = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedDonators).hasSameSizeAs(donationToSend);
    }

    @Test
    void shouldExtractDonatorsName() throws InterruptedException, ExecutionException, TimeoutException {
        Donation donation = new Donation("Foo", "Bar", 10.0, 111);

        Future<List<Donator>> receiveFuture = testClusterClient.waitForRecords("testConsumerGroup", 1,
                StringDeserializer.class);

        testClusterClient.sendRecords(Collections.singletonList(donation), StringSerializer.class,
                (index, event) -> String.valueOf(index));

        List<Donator> receivedDonators = receiveFuture.get(MAX_CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);

        assertThat(receivedDonators).hasSize(1);
        assertThat(receivedDonators.get(0).getName()).asString().isEqualTo("Foo Bar");
    }
}
package de.id.quarkus.kafka.testing;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A Quarkus test resource bootstrapping a Confluent Kafka stack incl. Kafka, schema registry and zookeeper by
 * testcontainers.<br>
 * Integrated as @{@link io.quarkus.test.common.QuarkusTestResource}<br>
 * <p>
 * Version of used Confluent can be customized as initArg: <br>
 * <b>@QuarkusTestResource(value = ConfluentStack.class, initArgs = { @ResourceArg(name = ConfluentStack
 * .CONFLUENT_VERSION_ARG, value = "5.3.1")})</b><br>
 * If not set CONFLUENT_VERSION_DEFAULT will be used. <br>
 * <p>
 * Automatically injects {@link ConfluentStackClient} on fields of the test suite (no annotation needed) <br>
 * Deletes all topics and consumer groups when injected to a new test instance.
 */
public class ConfluentStack implements QuarkusTestResourceLifecycleManager {

    public static final String CONFLUENT_VERSION_ARG = "confluentVersion";
    public static final String CONFLUENT_VERSION_DEFAULT = "5.4.3";
    private DockerImageName kafkaImage;
    private DockerImageName registryImage;
    String kafkaNetworkAlias = "kafka";
    String incoming;
    String incomingTopic;
    String outgoing;
    String outgoingTopic;
    Network network;
    KafkaContainer kafka;
    ConfluentSchemaRegistryContainer schemaRegistry;
    ConfluentStackClient testClusterClient;

    @Override
    public void init(Map<String, String> initArgs) {
        String confluentVersion = initArgs.getOrDefault(CONFLUENT_VERSION_ARG, CONFLUENT_VERSION_DEFAULT);
        this.kafkaImage = DockerImageName.parse(String.format("confluentinc/cp-kafka:%s", confluentVersion));
        this.registryImage = DockerImageName.parse(
                String.format("confluentinc/cp-schema-registry:%s", confluentVersion));
        this.incoming = initArgs.getOrDefault("incoming", "mb-source");
        this.incomingTopic = initArgs.getOrDefault("incomingTopic", "multibootstrap.source-topic");
        this.outgoing = initArgs.getOrDefault("outgoing", "mb-target");
        this.outgoingTopic = initArgs.getOrDefault("outgoingTopic", "multibootstrap.target-topic");
    }

    @Override
    public Map<String, String> start() {
        this.network = Network.newNetwork();

        this.kafka = new KafkaContainer(kafkaImage)
                .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
                .withNetwork(this.network)
                .withNetworkAliases(this.kafkaNetworkAlias);
        this.kafka.start();

        String dockerNetworkKafkaConnectString = String.format("%s:%d", this.kafkaNetworkAlias, 9092);
        this.schemaRegistry = new ConfluentSchemaRegistryContainer(registryImage, dockerNetworkKafkaConnectString)
                .withNetwork(this.network);
        this.schemaRegistry.start();

        testClusterClient = new ConfluentStackClient(kafka.getBootstrapServers(), this.schemaRegistry.getUrl());

        Map<String, String> properties = new HashMap<>();
        properties.put("kafka.bootstrap.servers", kafka.getBootstrapServers());

        properties.put(String.format("mp.messaging.incoming.%s.connector", incoming), "smallrye-kafka");
        properties.put(String.format("mp.messaging.incoming.%s.allow.auto.create.topics", incoming), "false");
        properties.put(String.format("mp.messaging.incoming.%s.topic", incoming), incomingTopic);
        properties.put(String.format("mp.messaging.incoming.%s.bootstrap.servers", incoming),
                kafka.getBootstrapServers());
        properties.put(String.format("mp.messaging.incoming.%s.schema.registry.url", incoming),
                this.schemaRegistry.getUrl());
        properties.put(String.format("mp.messaging.incoming.%s.broadcast", incoming), "true");

        properties.put(String.format("mp.messaging.outgoing.%s.connector", outgoing), "smallrye-kafka");
        properties.put(String.format("mp.messaging.outgoing.%s.allow.auto.create.topics", outgoing), "false");
        properties.put(String.format("mp.messaging.outgoing.%s.topic", outgoing), outgoingTopic);
        properties.put(String.format("mp.messaging.outgoing.%s.bootstrap.servers", outgoing),
                kafka.getBootstrapServers());
        properties.put(String.format("mp.messaging.outgoing.%s.schema.registry.url", outgoing),
                this.schemaRegistry.getUrl());
        properties.put(String.format("mp.messaging.outgoing.%s.merge", outgoing), "true");

        properties.put("mp.messaging.connector.smallrye-kafka.schema.registry.url", this.schemaRegistry.getUrl());
        properties.put("quarkus.kafka-streams.bootstrap-servers", kafka.getBootstrapServers());
        properties.put("quarkus.kafka-streams.schema-registry-url", this.schemaRegistry.getUrl());
        return properties;
    }

    @Override
    public void inject(Object testInstance) {
        injectClientInTestInstance(testInstance);
    }

    private void injectClientInTestInstance(Object testInstance) {
        Stream.of(testInstance.getClass(), testInstance.getClass().getSuperclass())
                .filter(Objects::nonNull)
                .flatMap(clazz -> Arrays.stream(clazz.getDeclaredFields()))
                .filter(field -> field.getType().equals(ConfluentStackClient.class))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(testInstance, testClusterClient);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(String.format("Error while injecting %s to instance %s",
                                ConfluentStackClient.class.getName(), testInstance), e);
                    }
                });
    }

    @Override
    public void stop() {
        this.kafka.close();
        this.schemaRegistry.close();
    }
}

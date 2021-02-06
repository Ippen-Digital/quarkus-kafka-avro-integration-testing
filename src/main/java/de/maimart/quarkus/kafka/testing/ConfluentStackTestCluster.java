package de.maimart.quarkus.kafka.testing;

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
 * Automatically injects ConfluentStackTestClusterClient on fields of the test suite (no annotations needed)
 */
public class ConfluentStackTestCluster implements QuarkusTestResourceLifecycleManager {

    public static final String CONFLUENT_VERSION_ARG = "confluentVersion";
    private DockerImageName kafkaImage;
    private DockerImageName registryImage;
    String kafkaNetworkAlias = "kafka";

    Network network;
    KafkaContainer kafka;
    ConfluentSchemaRegistryContainer schemaRegistry;
    ConfluentStackTestClusterClient testClusterClient;

    @Override
    public void init(Map<String, String> initArgs) {
        String confluentVersion = initArgs.getOrDefault(CONFLUENT_VERSION_ARG, "5.4.3");
        kafkaImage = DockerImageName.parse(String.format("confluentinc/cp-kafka:%s", confluentVersion));
        registryImage = DockerImageName.parse(String.format("confluentinc/cp-schema-registry:%s", confluentVersion));
    }

    @Override
    public Map<String, String> start() {
        network = Network.newNetwork();

        this.kafka = new KafkaContainer(kafkaImage)
                .withNetwork(network)
                .withNetworkAliases(kafkaNetworkAlias);
        this.kafka.start();

        String dockerNetworkKafkaConnectString = String.format("%s:%d", kafkaNetworkAlias, 9092);
        schemaRegistry = new ConfluentSchemaRegistryContainer(registryImage, dockerNetworkKafkaConnectString)
                .withNetwork(network);
        schemaRegistry.start();

        testClusterClient = new ConfluentStackTestClusterClient(kafka.getBootstrapServers(), schemaRegistry.getUrl());

        Map<String, String> properties = new HashMap<>();
        properties.put("kafka.bootstrap.servers", kafka.getBootstrapServers());
        properties.put("mp.messaging.connector.smallrye-kafka.schema.registry.url", schemaRegistry.getUrl());
        return properties;
    }

    @Override
    public void inject(Object testInstance) {
        testClusterClient.deleteAllTopics();
        injectClientInTestInstance(testInstance);
    }

    private void injectClientInTestInstance(Object testInstance) {
        Stream.of(testInstance.getClass(), testInstance.getClass().getSuperclass())
                .filter(Objects::nonNull)
                .flatMap(clazz -> Arrays.stream(clazz.getDeclaredFields()))
                .filter(field -> field.getType().equals(ConfluentStackTestClusterClient.class))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(testInstance, testClusterClient);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(String.format("Error while injecting %s to instance %s", ConfluentStackTestClusterClient.class.getName(), testInstance), e);
                    }
                });
    }

    @Override
    public void stop() {
        kafka.close();
        schemaRegistry.close();
    }
}

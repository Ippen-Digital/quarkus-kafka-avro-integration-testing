package de.id.quarkus.kafka.testing;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
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
    public static final String DEFAULT_SOURCE_TOPIC = "reactivemessaging.source-topic";
    public static final String DEFAULT_TARGET_TOPIC = "reactivemessaging.target-topic";
    private DockerImageName kafkaImage;
    private DockerImageName registryImage;
    String kafkaNetworkAlias = "kafka";
    String incoming;
    String outgoing;
    String sourceTopic;
    String targetTopic;
    Network network;
    KafkaContainer kafka;
    ConfluentSchemaRegistryContainer schemaRegistry;
    ConfluentStackClient testClusterClient;

    @Override
    public void init(Map<String, String> initArgs) {
        String confluentVersion = initArgs.getOrDefault(CONFLUENT_VERSION_ARG, getConfluentVersionDefault());
        this.kafkaImage = DockerImageName.parse(String.format("confluentinc/cp-kafka:%s", confluentVersion));
        this.registryImage = DockerImageName.parse(
                String.format("confluentinc/cp-schema-registry:%s", confluentVersion));
        this.incoming = initArgs.get("incoming");
        this.outgoing = initArgs.get("outgoing");
        this.sourceTopic = initArgs.getOrDefault("sourceTopic", DEFAULT_SOURCE_TOPIC);
        this.targetTopic = initArgs.getOrDefault("targetTopic", DEFAULT_TARGET_TOPIC);

        this.network = Network.newNetwork();
        this.kafka = new KafkaContainer(kafkaImage)
                .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
                .withNetwork(this.network)
                .withNetworkAliases(this.kafkaNetworkAlias);
        String dockerNetworkKafkaConnectString = String.format("%s:%d", this.kafkaNetworkAlias, 9092);
        this.schemaRegistry = new ConfluentSchemaRegistryContainer(registryImage, dockerNetworkKafkaConnectString)
                .withNetwork(this.network);
    }

    String getConfluentVersionDefault() {
        String cpuArch = System.getProperty("os.arch");
        if (cpuArch != null && cpuArch.contains("aarch64")) {
            return "7.2.1.arm64";
        } else {
            return "7.2.1";
        }
    }

    @Override
    public Map<String, String> start() {
        if (this.testClusterClient == null && this.kafka != null) {
            this.kafka.start();
            this.schemaRegistry.start();
            testClusterClient = new ConfluentStackClient(kafka.getBootstrapServers(), this.schemaRegistry.getUrl(), sourceTopic, targetTopic);
            testClusterClient.createTopics(sourceTopic, targetTopic);

            Map<String, String> properties = new HashMap<>();
            properties.put("kafka.bootstrap.servers", kafka.getBootstrapServers());


            if (incoming != null) {
                properties.put(String.format("mp.messaging.incoming.%s.connector", incoming), "smallrye-kafka");
                properties.put(String.format("mp.messaging.incoming.%s.allow.auto.create.topics", incoming), "false");
                properties.put(String.format("mp.messaging.incoming.%s.topic", incoming), sourceTopic);
                properties.put(String.format("mp.messaging.incoming.%s.bootstrap.servers", incoming),
                        kafka.getBootstrapServers());
                properties.put(String.format("mp.messaging.incoming.%s.schema.registry.url", incoming),
                        this.schemaRegistry.getUrl());
            }

            if (outgoing != null) {
                properties.put(String.format("mp.messaging.outgoing.%s.connector", outgoing), "smallrye-kafka");
                properties.put(String.format("mp.messaging.outgoing.%s.allow.auto.create.topics", outgoing), "false");
                properties.put(String.format("mp.messaging.outgoing.%s.topic", outgoing), targetTopic);
                properties.put(String.format("mp.messaging.outgoing.%s.bootstrap.servers", outgoing),
                        kafka.getBootstrapServers());
                properties.put(String.format("mp.messaging.outgoing.%s.schema.registry.url", outgoing),
                        this.schemaRegistry.getUrl());
            }

            properties.put("mp.messaging.connector.smallrye-kafka.schema.registry.url", this.schemaRegistry.getUrl());
            properties.put("quarkus.kafka-streams.bootstrap-servers", kafka.getBootstrapServers());
            properties.put("quarkus.kafka-streams.schema-registry-url", this.schemaRegistry.getUrl());
            return properties;
        } else {
            return Collections.emptyMap();
        }
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
        if (kafka != null) {
            kafka.close();
        }
        if (schemaRegistry != null) {
            schemaRegistry.close();
        }
    }
}

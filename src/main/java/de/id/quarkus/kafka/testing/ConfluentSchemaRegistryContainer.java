package de.id.quarkus.kafka.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static java.lang.String.format;

/**
 * Confluent schema registry container
 */
class ConfluentSchemaRegistryContainer extends GenericContainer<ConfluentSchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

    private static final String NETWORK_ALIAS = "schema-registry";

    ConfluentSchemaRegistryContainer(DockerImageName dockerImageName, String internalKafkaConnectString) {
        super(dockerImageName);

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", internalKafkaConnectString);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases(NETWORK_ALIAS);

        waitingFor(Wait.forHttp("/subjects"));
    }

    String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }
}

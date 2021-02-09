package de.id.quarkus.kafka.testing;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A client facilitating access to the {@link ConfluentStack}.
 */
public class ConfluentStackClient {

    private final String kafkaBootstrapServers;
    private final String schemaRegistryUrl;

    ConfluentStackClient(String kafkaBootstrapServers, String schemaRegistryUrl) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    /**
     * synchronously deletes all existing topics
     */
    public void deleteAllTopics() {
        AdminClient adminClient = createAdminClient();
        try {
            adminClient.listTopics().names().thenApply(adminClient::deleteTopics).get();
        } catch (Exception e) {
            throw new RuntimeException("Error while deleting topics", e);
        }
    }

    /**
     * create topic(s) with 1 partition and 1 replica
     *
     * @param topicNames name of topics
     */
    public void createTopics(String... topicNames) {
        List<NewTopic> newTopics = Arrays.stream(topicNames).map(topicName -> new NewTopic(topicName, 1, (short) 1)).collect(Collectors.toList());
        try {
            createAdminClient().createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Error while creating topics", e);
        }
    }

    /**
     * synchronously deletes all consumer groups
     */
    public void deleteAllConsumerGroups() {
        AdminClient adminClient = createAdminClient();
        try {
            adminClient.listConsumerGroups().all().thenApply(consumerGroupListings -> {
                List<String> groupIds = consumerGroupListings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                return adminClient.deleteConsumerGroups(groupIds);
            }).get();
        } catch (Exception e) {
            throw new RuntimeException("Error while deleting consumer groups", e);
        }
    }

    /**
     * register a new schema to the schema registry
     *
     * @param schema schema to be registerd
     */
    public void registerSchemaRegistryTypes(Schema schema) {
        try {
            schemaRegistryClient().register(schema.getFullName(), schema);
        } catch (Exception e) {
            throw new RuntimeException("Error while registering schemas", e);
        }
    }

    /**
     * @return preconfigured client for the Kafka
     */
    public AdminClient createAdminClient() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        return KafkaAdminClient.create(properties);
    }

    /**
     * @return preconfigured client for the schema registry
     */
    public CachedSchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient(
                getSchemaRegistryUrl(), 1000);
    }

    /**
     * Creates a producer using a {@link SpecificAvroSerializer} for the value, configured for the schema registry.
     *
     * @param keySerializerClass serializer used for the record key
     * @param <K>                type of the key
     * @param <V>                type of the value
     * @return ready to use kafka producer
     */
    public <K, V> KafkaProducer<K, V> createProducerWithAvroValue(Class<? extends Serializer<K>> keySerializerClass) {
        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a producer using a {@link SpecificAvroDeserializer} for the value, configured for the schema registry.
     *
     * @param keyDeserializer       serializer used for the record key
     * @param consumerGroupIdPrefix prefix for the consumer group
     * @param <K>                   type of the key
     * @param <V>                   type of the value
     * @return ready to user kafka consumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumerWithAvroValue(Class<? extends Deserializer<K>> keyDeserializer, String consumerGroupIdPrefix) {
        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupIdPrefix + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);

        return new KafkaConsumer<>(props);
    }

    /**
     * send records to the kafka topic using a producer build by {@link #createProducerWithAvroValue(Class)}
     *
     * @param topic               target topic
     * @param values              list of values that will be sent
     * @param keySerializerClass  serializer used for the record key
     * @param keyCreationFunction function used for creating the record key
     * @param <K>                 type of the key
     * @param <V>                 type of the value
     */
    public <K, V> void sendRecords(String topic, List<V> values, Class<? extends Serializer<K>> keySerializerClass, BiFunction<Integer, V, K> keyCreationFunction) {
        KafkaProducer<K, V> producer = createProducerWithAvroValue(keySerializerClass);

        for (int i = 0, valuesSize = values.size(); i < valuesSize; i++) {
            V object = values.get(i);
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, keyCreationFunction.apply(i, object), object);
            producer.send(record);
        }
        producer.flush();
    }

    /**
     * waiting to receive records from the kafka topic using a consumber build by {@link #createConsumerWithAvroValue(Class, String)}
     *
     * @param topicName       consuming topic
     * @param groupIdPrefix   prefix used for the groupId (a unifier will be added)
     * @param maxWaitTimeInMs maximum time the function will wait until expectedItems are received
     * @param expectedItems   minimum amount of items waiting to be received
     * @param keyDeserializer key used for the key record
     * @param <K>             type of key
     * @param <V>             type of value
     * @return received records, not null
     */
    public <K, V> List<V> waitForRecords(String topicName, String groupIdPrefix, int maxWaitTimeInMs, int expectedItems, Class<? extends Deserializer<K>> keyDeserializer) {
        Instant startTime = Instant.now();

        KafkaConsumer<K, V> recoConsumer = createConsumerWithAvroValue(keyDeserializer, groupIdPrefix);
        recoConsumer.subscribe(Collections.singletonList(topicName));

        List<V> receivedStoryRecommendations = new ArrayList<>();
        do {
            ConsumerRecords<K, V> consumedRecords = recoConsumer.poll(Duration.ofMillis(1000));
            consumedRecords.records(topicName).forEach(record -> receivedStoryRecommendations.add(record.value()));
        } while (Instant.now().toEpochMilli() - startTime.toEpochMilli() < maxWaitTimeInMs && receivedStoryRecommendations.size() < expectedItems);
        return receivedStoryRecommendations;
    }
}

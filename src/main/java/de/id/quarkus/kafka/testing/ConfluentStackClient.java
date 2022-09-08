package de.id.quarkus.kafka.testing;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A client facilitating access to the {@link ConfluentStack}.
 */
public class ConfluentStackClient {

    private final String kafkaBootstrapServers;
    private final String schemaRegistryUrl;
    private final ExecutorService executor;
    private final String sourceTopic;
    private final String targetTopic;

    ConfluentStackClient(String kafkaBootstrapServers, String schemaRegistryUrl, String sourceTopic,
                         String targetTopic) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.executor = Executors.newCachedThreadPool();
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    /**
     * create topic(s) with 1 partition and 1 replica, if they are not already existing
     *
     * @param topicNames name of topics
     */
    public void createTopics(String... topicNames) {

        try {
            AdminClient adminClient = createAdminClient();
            Set<String> alreadyExistingTopics = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5000))
                    .names()
                    .get();
            List<NewTopic> topicsToCreate = Arrays.stream(topicNames)
                    .filter(topicName -> !alreadyExistingTopics.contains(topicName))
                    .map(topicName -> new NewTopic(topicName, 1, (short) 1))
                    .collect(Collectors.toList());
            adminClient.createTopics(topicsToCreate).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Error while creating topics", e);
        }
    }

    /**
     * @return preconfigured client for the Kafka
     */
    public AdminClient createAdminClient() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "TestAdminClient-" + UUID.randomUUID());
        return KafkaAdminClient.create(properties);
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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TestProducer-" + UUID.randomUUID());
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
    public <K, V> KafkaConsumer<K, V> createConsumerWithAvroValue(Class<? extends Deserializer<K>> keyDeserializer,
                                                                  String consumerGroupIdPrefix) {
        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupIdPrefix + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);

        return new KafkaConsumer<>(props);
    }

    /**
     * send records to the source kafka topic defined in constructor using a producer build by
     * {@link #createProducerWithAvroValue(Class)}
     *
     * @param <K>                 type of the key
     * @param <V>                 type of the value
     * @param values              list of values that will be sent
     * @param keySerializerClass  serializer used for the record key
     * @param keyCreationFunction function used for creating the record key
     */
    public <K, V> void sendRecords(List<V> values, Class<? extends Serializer<K>> keySerializerClass,
                                   BiFunction<Integer, V, K> keyCreationFunction) {
        sendRecordsToTopic(sourceTopic, values, keySerializerClass, keyCreationFunction);
    }

    /***
     * send records to a specific kafka topic using a producer build by {@link #createProducerWithAvroValue(Class)}
     *
     * @param topic               a kafka topic name
     * @param <K>                 type of the key
     * @param <V>                 type of the value
     * @param values              list of values that will be sent
     * @param keySerializerClass  serializer used for the record key
     * @param keyCreationFunction function used for creating the record key
     */
    public <K, V> void sendRecordsToTopic(String topic, List<V> values,
                                          Class<? extends Serializer<K>> keySerializerClass, BiFunction<Integer, V,
            K> keyCreationFunction) {
        KafkaProducer<K, V> producer = createProducerWithAvroValue(keySerializerClass);

        for (int i = 0, valuesSize = values.size(); i < valuesSize; i++) {
            V object = values.get(i);
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, keyCreationFunction.apply(i, object), object);
            producer.send(record);
        }
        producer.flush();
    }

    /**
     * waiting to receive records from the target kafka topic defined in constructor using a consumer build by
     * {@link #createConsumerWithAvroValue(Class, String)}
     *
     * @param <K>             type of key
     * @param <V>             type of value
     * @param groupIdPrefix   prefix used for the groupId (a unifier will be added)received
     * @param expectedItems   minimum amount of items waiting to be received
     * @param keyDeserializer key used for the key record
     * @return received records, not null
     */
    public <K, V> Future<List<V>> waitForRecords(String groupIdPrefix, int expectedItems, Class<?
            extends Deserializer<K>> keyDeserializer) {
        return waitForRecordsFromTopic(targetTopic, groupIdPrefix, expectedItems, keyDeserializer);
    }

    /**
     * waiting to receive records from a specific kafka topic using a consumer build by
     * {@link #createConsumerWithAvroValue(Class, String)}
     *
     * @param topicName       a kafka topic name
     * @param <K>             type of key
     * @param <V>             type of value
     * @param groupIdPrefix   prefix used for the groupId (a unifier will be added)received
     * @param expectedItems   minimum amount of items waiting to be received
     * @param keyDeserializer key used for the key record
     * @return received records, not null
     */
    public <K, V> Future<List<V>> waitForRecordsFromTopic(String topicName, String groupIdPrefix, int expectedItems,
                                                          Class<? extends Deserializer<K>> keyDeserializer) {
        // it´s important to subscribe synchronous to avoid race conditions when afterwards a producer writes to this
        // topic
        KafkaConsumer<K, V> recoConsumer = this.createConsumerWithAvroValue(keyDeserializer, groupIdPrefix);
        recoConsumer.subscribe(Collections.singletonList(topicName));
        return executor.submit(() -> {
            List<V> receivedStoryRecommendations = new ArrayList<>();
            do {
                ConsumerRecords<K, V> consumedRecords = recoConsumer.poll(Duration.ofMillis(1000));
                consumedRecords.records(topicName).forEach(record -> receivedStoryRecommendations.add(record.value()));
            } while (receivedStoryRecommendations.size() < expectedItems);
            return receivedStoryRecommendations;
        });
    }
}

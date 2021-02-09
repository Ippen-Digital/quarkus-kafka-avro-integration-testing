# quarkus-kafka-avro-integration-testing

**A testing library for Quarkus projects implementing Kafka with Avro.**

## What does this testing library provide?

### A Quarkus TestResource bootstrapping a complete [Confluent Kafka](https://www.confluent.io/) stack
* including the zookeeper, kafka and a schema registry 
* started as docker containers using [testcontainers](https://www.testcontainers.org/)
* automatically overrides config properties
  * kafka.bootstrap.servers
  * mp.messaging.connector.smallrye-kafka.schema.registry.url
* version can be customized
* topics and consumer groups will be deleted for every test suite

### An auto injected client leveraging

* creation and deletion of topics
* registering and deleting schemas
* factory functions for kafka producers and consumers
* high level functions for direct sending and receiving of events
* creation of Kafka admin client
* creation of schema registry client

## How to install?
### add dependency
```xml
<dependency>
    <groupId>de.ippen-digital</groupId>
    <artifactId>quarkus-kafka-avro-integration-testing</artifactId>
    <version>1.0.0</version>
    <scope>test</scope>
</dependency>
```
  
## How to configure
### add testing resource

```java
@QuarkusTest
@QuarkusTestResource(value = ConfluentStack.class)
class YourIntegrationTest {}
```

Version of the Confluent stack can be customized
```java
@QuarkusTestResource(value = ConfluentStack.class, initArgs = {@ResourceArg(name = ConfluentStack.CONFLUENT_VERSION_ARG, value = "5.3.1")})
```

### inject the client

Just define the field in the test suite, no annotation needed

```java
public ConfluentStackClient confluentStackClient;
```

## How to use the client?

### Read the JavaDoc of the ConfluentStackClient

The ConfluentStackClient is well documented

### Example

See [KafkaStreamExampleTest.java](src/test/java/de/id/quarkus/kafka/testing/KafkaStreamExampleTest.java) as a real example used for testing this test library
prove dependencies (to be excluded from package), more information to maven dependency above
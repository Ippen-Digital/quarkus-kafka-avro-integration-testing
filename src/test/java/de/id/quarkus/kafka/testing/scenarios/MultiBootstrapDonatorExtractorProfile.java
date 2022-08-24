package de.id.quarkus.kafka.testing.scenarios;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class MultiBootstrapDonatorExtractorProfile implements QuarkusTestProfile {

    public static final String PROFILE_NAME = "multi-boostrap-donator-extractor";

    @Override
    public String getConfigProfile() {
        return PROFILE_NAME;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        var incoming = "source-topic";
        var incomingTopic = "reactivemessaging.source-topic";
        var outgoing = "target-topic";
        var outgoingTopic = "reactivemessaging.target-topic";
        Map<String, String> properties = new HashMap<>();
        properties.put(String.format("mp.messaging.incoming.%s.topic", incoming), incomingTopic);
        properties.put(String.format("mp.messaging.incoming.%s.broadcast", incoming), "true");
        properties.put(String.format("mp.messaging.outgoing.%s.topic", outgoing), outgoingTopic);
        properties.put(String.format("mp.messaging.outgoing.%s.merge", outgoing), "true");
        return properties;
    }

}

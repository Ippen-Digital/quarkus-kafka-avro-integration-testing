package de.id.quarkus.kafka.testing.scenarios;

import de.id.quarkus.kafka.testing.ConfluentStack;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiBootstrapDonatorExtractorProfile implements QuarkusTestProfile {

    public static final String PROFILE_NAME = "multi-boostrap-donator-extractor";

    @Override
    public String getConfigProfile() {
        return PROFILE_NAME;
    }

    @Override
    public List<TestResourceEntry> testResources() {
        Map<String, String> initArgs = new HashMap<>();
        initArgs.put("incoming", "mb-source");
        initArgs.put("incomingTopic", "multibootstrap.source-topic");
        initArgs.put("outgoing", "mb-target");
        initArgs.put("outgoingTopic", "multibootstrap.target-topic");
        return List.of(new TestResourceEntry(ConfluentStack.class, initArgs));
    }
}

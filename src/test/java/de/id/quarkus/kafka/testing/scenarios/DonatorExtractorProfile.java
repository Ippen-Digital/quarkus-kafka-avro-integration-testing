package de.id.quarkus.kafka.testing.scenarios;

import io.quarkus.test.junit.QuarkusTestProfile;

public class DonatorExtractorProfile implements QuarkusTestProfile {

    public static final String PROFILE_NAME = "donator-extractor";

    @Override
    public String getConfigProfile() {
        return PROFILE_NAME;
    }
}

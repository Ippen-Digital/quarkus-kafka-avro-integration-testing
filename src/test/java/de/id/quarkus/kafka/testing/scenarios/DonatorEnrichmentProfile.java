package de.id.quarkus.kafka.testing.scenarios;

import io.quarkus.test.junit.QuarkusTestProfile;

public class DonatorEnrichmentProfile implements QuarkusTestProfile {

    public static final String PROFILE_NAME = "donator-enrichment";

    @Override
    public String getConfigProfile() {
        return PROFILE_NAME;
    }
}

package de.maimart.quarkus.kafka.testing;

import io.quarkus.test.common.QuarkusTestResource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@QuarkusTestResource(value = ConfluentStackTestCluster.class)
public @interface ConfluentStackIntegrationTest {
}

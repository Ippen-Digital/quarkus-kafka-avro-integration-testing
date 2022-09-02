package de.id.quarkus.kafka.testing.reactivemessaging_multibootstrap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CpuArchTest {
    @Test
    void shouldReturnArm() {
        var arch = System.getProperty("os.arch");
        assertThat(arch).contains("aarch64");
    }
}

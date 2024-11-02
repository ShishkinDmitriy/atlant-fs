package org.atlantfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class AtlantFileSystemTest {

    AtlantFileSystem fileSystem;

    @BeforeEach
    void beforeEach() throws IOException {
    }

    @Test
    void new_shouldOpen(@Mock AtlantFileSystemProvider fileSystemProvider) throws IOException {
        // Given
        Path storage = Paths.get("test.fs");
        // When
        try (AtlantFileSystem fileSystem = new AtlantFileSystem(fileSystemProvider, storage, Map.of())) {
            // Then
            assertThat(fileSystem.isOpen()).isTrue();
        }
    }

    @AfterEach
    void afterEach() throws IOException {
        // When
        fileSystem.close();
        // Then
        assertThat(fileSystem.isOpen()).isFalse();
    }

    @Nested
    class WhenCreated {

    }

}

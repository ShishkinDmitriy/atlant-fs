package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({MockitoExtension.class, LoggingExtension.class})
class AtlantFileSystemTest {

    private AtlantFileSystem fileSystem;
    private @Mock AtlantFileSystemProvider fileSystemProvider;

    @BeforeEach
    void beforeEach() throws IOException {
        // Given
        var storage = Paths.get("test.fs");
        // When
        fileSystem = new AtlantFileSystem(fileSystemProvider, storage, Map.of());
    }

    @Test
    void new_shouldOpen() {
        // Then
        assertThat(fileSystem.isOpen()).isTrue();
    }

    @Nested
    class WhenCreated {

        @Test
        void new_shouldOpen() {
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

}

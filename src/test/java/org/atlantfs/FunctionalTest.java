package org.atlantfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FunctionalTest {

    @Test
    void newFileSystem_shouldCreateNewInstance() throws IOException {
        // Given
        var uri = URI.create("atlant:test.fs!/");
        // When
        var fileSystem = FileSystems.newFileSystem(uri, Map.of());
        // Then
        assertThat(fileSystem).isNotNull();
        assertThat(fileSystem.isOpen()).isTrue();
        assertThat(fileSystem.isReadOnly()).isFalse();
    }

}

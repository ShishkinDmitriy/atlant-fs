package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(LoggingExtension.class)
class FunctionalTest {

    @Test
    void newFileSystem_should_createNewInstance() throws IOException {
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

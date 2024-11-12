package org.atlantfs.func;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(LoggingExtension.class)
class SoapOperaTest {

    @Test
    void newFileSystem_should_createNewInstance() throws IOException {
        // Given
        var uri = URI.create("atlant:build/FunctionalTest.atlant!/");
        // When
        var fileSystem = FileSystems.newFileSystem(uri, Map.of());
        // Then
        assertThat(fileSystem).isNotNull();
        assertThat(fileSystem.isOpen()).isTrue();
        assertThat(fileSystem.isReadOnly()).isFalse();
    }

}

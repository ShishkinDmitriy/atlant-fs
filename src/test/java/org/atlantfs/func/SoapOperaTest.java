package org.atlantfs.func;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(LoggingExtension.class)
class SoapOperaTest {

    private static final String ATLANT_FILE_NAME = "build/CreateDirectoryTest2.atlant";
    private static final Path ATLANT_FILE = Paths.get(ATLANT_FILE_NAME);
    private static final URI ATLANT_URI = URI.create("atlant:" + ATLANT_FILE_NAME + "!/");
    private static final Map<String, Object> DEFAULT_CONFIG = Map.of();

    @BeforeEach
    void beforeEach() throws IOException {
        if (Files.exists(ATLANT_FILE)) {
            Files.delete(ATLANT_FILE);
        }
    }

    @Test
    void newFileSystem_should_createNewInstance() throws IOException {
        // When
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, DEFAULT_CONFIG)) {
            // Then
            assertThat(fileSystem).isNotNull();
            assertThat(fileSystem.isOpen()).isTrue();
            assertThat(fileSystem.isReadOnly()).isFalse();
        }
    }

}

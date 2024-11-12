package org.atlantfs.func;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(LoggingExtension.class)
class CreateFileSystemTest {

    public static final String ATLANT_FILE_NAME = "build/CreateFileSystemTest.atlant";
    public static final Path ATLANT_FILE = Paths.get(ATLANT_FILE_NAME);
    public static final URI ATLANT_URI = URI.create("atlant:" + ATLANT_FILE_NAME + "!/");

    @BeforeEach
    void beforeEach() throws IOException {
        if (Files.exists(ATLANT_FILE)) {
            Files.delete(ATLANT_FILE);
        }
    }

    @Test
    void createFileSystem_when_noExistingAtlantFile() throws IOException {
        // When
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, Map.of())) {
            // Then
            assertThat(fileSystem).isNotNull();
            // When
            try (var fileSystem2 = FileSystems.getFileSystem(ATLANT_URI)) {
                // Then
                assertThat(fileSystem2).isNotNull().isSameAs(fileSystem);
            }
        }
        assertThatThrownBy(() -> FileSystems.getFileSystem(ATLANT_URI))
                .isInstanceOf(FileSystemNotFoundException.class);
    }
}

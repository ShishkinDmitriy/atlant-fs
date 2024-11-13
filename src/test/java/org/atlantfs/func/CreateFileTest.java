package org.atlantfs.func;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(LoggingExtension.class)
class CreateFileTest {

    private static final String ATLANT_FILE_NAME = "build/CreateFileTest.atlant";
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
    void createFileSystem_when_noExistingAtlantFile() throws IOException {
        // Given
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, DEFAULT_CONFIG)) {
            var path = fileSystem.getPath("/file.txt");
            // When
            Files.createFile(path);
            // Then
            var files = collectFiles(fileSystem);
            assertThat(files).contains(path);
        }
    }

    private static ArrayList<Path> collectFiles(FileSystem fileSystem) throws IOException {
        var files = new ArrayList<Path>();
        Files.walkFileTree(fileSystem.getPath("/"), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                files.add(file);
                return super.visitFile(file, attrs);
            }
        });
        return files;
    }

}

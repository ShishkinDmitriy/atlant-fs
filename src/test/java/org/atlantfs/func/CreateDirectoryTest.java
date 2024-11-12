package org.atlantfs.func;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
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
class CreateDirectoryTest {

    public static final String ATLANT_FILE_NAME = "build/CreateDirectoryTest2.atlant";
    public static final Path ATLANT_FILE = Paths.get(ATLANT_FILE_NAME);
    public static final URI ATLANT_URI = URI.create("atlant:" + ATLANT_FILE_NAME + "!/");
    public static final int DIRS_COUNT = 3;

    @BeforeEach
    void beforeEach() throws IOException {
        if (Files.exists(ATLANT_FILE)) {
            Files.delete(ATLANT_FILE);
        }
    }

    /**
     * Creates {@code /level-0/level-1/level-3} and check their subdirectories.
     */
    @Test
    void createDirectory_depth() throws IOException {
        // Given
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, Map.of())) {
            var root = fileSystem.getPath("/");
            var path = root;
            for (int i = 0; i < DIRS_COUNT; i++) {
                path = path.resolve("level-" + i);
            }
            // When
            Files.createDirectories(path);
            // Then
            var current = path;
            while (!current.equals(root)) {
                var parent = current.getParent();
                try (var children = Files.newDirectoryStream(parent)) {
                    var iterator = children.iterator();
                    assertThat(iterator.hasNext()).isTrue();
                    assertThat(iterator.next()).isEqualTo(current);
                    assertThat(iterator.hasNext()).isFalse();
                }
                current = parent;
            }
            try (var children = Files.newDirectoryStream(root)) {
                var iterator = children.iterator();
                assertThat(iterator.hasNext()).isTrue();
                assertThat(iterator.next()).isEqualTo(fileSystem.getPath("/level-0"));
                assertThat(iterator.hasNext()).isFalse();
            }
        }
        // When
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, Map.of())) { // Reopen
            var root = fileSystem.getPath("/");
            var path = root;
            for (int i = 0; i < DIRS_COUNT; i++) {
                path = path.resolve("level-" + i);
            }
            // Then
            var current = path;
            while (!current.equals(root)) {
                var parent = current.getParent();
                try (var children = Files.newDirectoryStream(parent)) {
                    var iterator = children.iterator();
                    assertThat(iterator.hasNext()).isTrue();
                    assertThat(iterator.next()).isEqualTo(current);
                    assertThat(iterator.hasNext()).isFalse();
                }
                current = parent;
            }
            try (var children = Files.newDirectoryStream(root)) {
                var iterator = children.iterator();
                assertThat(iterator.hasNext()).isTrue();
                assertThat(iterator.next()).isEqualTo(fileSystem.getPath("/level-0"));
                assertThat(iterator.hasNext()).isFalse();
            }
        }
    }


    /**
     * Creates {@code /level-0}, {@code /level-2}, {@code /level-3} and check their subdirectories.
     */
    @Test
    void createDirectory_breadth() throws IOException {
        // Given
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, Map.of())) {
            var paths = new ArrayList<Path>();
            for (int i = 0; i < DIRS_COUNT; i++) {
                paths.add(fileSystem.getPath("/level-" + i));
            }
            // When
            for (Path path : paths) {
                Files.createDirectories(path);
            }
            // Then
            var agg = new ArrayList<Path>();
            try (var children = Files.newDirectoryStream(fileSystem.getPath("/"))) {
                for (Path child : children) {
                    agg.add(child);
                }
            }
            assertThat(agg).hasSize(DIRS_COUNT).containsExactlyInAnyOrderElementsOf(paths);
        }
    }

    @Test
    void soapOpera() throws IOException {
        // Given
        var collection = new ArrayList<String>();
        var projectDir = Paths.get(System.getProperty("project.dir"));
        try (var fileSystem = FileSystems.newFileSystem(ATLANT_URI, Map.of())) {
            // When
            Files.walkFileTree(projectDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    var normalized = normalize(dir);
                    var path = fileSystem.getPath(normalized);
                    Files.createDirectories(path);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            var actual = new ArrayList<String>();
            Files.walkFileTree(fileSystem.getPath(normalize(projectDir)), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    actual.add(normalize(dir));
                    return FileVisitResult.CONTINUE;
                }
            });
            assertThat(actual).containsExactlyInAnyOrderElementsOf(collection);
        }
    }

    private String normalize(Path path) {
        return path.toString()
                .replaceFirst("C:\\\\", "/")
                .replace('\\', '/');
    }

}

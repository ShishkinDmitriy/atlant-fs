package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.atlantfs.util.AtlantFileUtil.atlantRoot;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.PathUtil.allDirectories;
import static org.atlantfs.util.PathUtil.normalize;
import static org.atlantfs.util.PathUtil.projectDir;

@ExtendWith(LoggingExtension.class)
class CreateDirectoryTest {

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    /**
     * Creates {@code /level-0/level-1/level-3} and check their subdirectories.
     */
    @Test
    void createDirectory_depth(TestInfo testInfo) throws IOException {
        // Given
        var dirsCount = 3;
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults();
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var root = fileSystem.getPath("/");
            var path = root;
            for (int i = 0; i < dirsCount; i++) {
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
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, AtlantConfig.defaults().asMap())) { // Reopen
            var root = fileSystem.getPath("/");
            var path = root;
            for (int i = 0; i < dirsCount; i++) {
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
    void createDirectory_breadth(TestInfo testInfo) throws IOException {
        // Given
        var dirsCount = 3;
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults();
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var paths = new ArrayList<Path>();
            for (int i = 0; i < dirsCount; i++) {
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
            assertThat(agg).hasSize(dirsCount).containsExactlyInAnyOrderElementsOf(paths);
        }
        // When
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var paths = new ArrayList<Path>();
            for (int i = 0; i < dirsCount; i++) {
                paths.add(fileSystem.getPath("/level-" + i));
            }
            // Then
            var agg = new ArrayList<Path>();
            try (var children = Files.newDirectoryStream(fileSystem.getPath("/"))) {
                for (Path child : children) {
                    agg.add(child);
                }
            }
            assertThat(agg).hasSize(dirsCount).containsExactlyInAnyOrderElementsOf(paths);
        }
    }

    @Test
    void soapOpera(TestInfo testInfo) throws IOException {
        // Given
        var collection = new ArrayList<String>();
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(1024)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(32);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // When
            Files.walkFileTree(projectDir(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (dir.equals(atlantRoot())) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    var normalized = normalize(dir);
                    var path = fileSystem.getPath(normalized);
                    Files.createDirectories(path);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            assertThat(allDirectories(fileSystem)).containsExactlyInAnyOrderElementsOf(collection);
        }
    }

}

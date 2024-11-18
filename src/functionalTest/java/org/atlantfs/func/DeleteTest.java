package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
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
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.PathUtil.allDirectories;
import static org.atlantfs.util.PathUtil.allRegularFiles;
import static org.atlantfs.util.PathUtil.buildDir;
import static org.atlantfs.util.PathUtil.normalize;
import static org.atlantfs.util.PathUtil.projectDir;

@ExtendWith(LoggingExtension.class)
class DeleteTest {

    private static final Logger log = Logger.getLogger(DeleteTest.class.getName());

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @RepeatedTest(3)
    void delete_should_freeBlocks_when_smallContent(TestInfo testInfo) throws IOException {
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        var text = "test";
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            Files.writeString(path, text, CREATE);
            assertThat(Files.readString(path)).isEqualTo(text);
            // When
            Files.delete(path);
            // Then
            assertThat(path).doesNotExist();
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // Then
            assertThat(path).doesNotExist();
        }
    }

    @RepeatedTest(3)
    void delete_should_freeBlocks_when_bigContent(TestInfo testInfo) throws IOException {
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        var text = """
                Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut \
                labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores \
                et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem \
                ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et \
                dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea \
                rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.
                """;
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            Files.writeString(path, text, CREATE);
            assertThat(Files.readString(path)).isEqualTo(text);
            // When
            Files.delete(path);
            // Then
            assertThat(path).doesNotExist();
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // Then
            assertThat(path).doesNotExist();
        }
    }

    @RepeatedTest(3)
    void delete_should_persistContent_when_bigBigContent(TestInfo testInfo) throws IOException { // File should be > Files::BUFFER_SIZE
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        var license = projectDir().resolve("LICENSE");
        var text = Files.readString(license);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/LICENSE");
            Files.writeString(path, text, CREATE);
            assertThat(Files.readString(path)).isEqualTo(text);
            // When
            Files.delete(path);
            // Then
            assertThat(path).doesNotExist();
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/LICENSE");
            // Then
            assertThat(path).doesNotExist();
        }
    }

    @Test
    void soapOpera(TestInfo testInfo) throws IOException {
        // Given
        var collection = new ArrayList<String>();
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(512)
                .inodeSize(32)
                .numberOfBlockBitmaps(3)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(80);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // Given
            Files.walkFileTree(projectDir(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (dir.equals(buildDir())) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toString().endsWith(".lock")) {
                        // Can't open as already opened
                        return FileVisitResult.CONTINUE;
                    }
                    log.fine(() -> "Writing [file=" + file + "]...");
                    var normalized = normalize(file);
                    var bytes = Files.readAllBytes(file);
                    Files.write(fileSystem.getPath(normalized), bytes, CREATE);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            assertThat(allRegularFiles(fileSystem)).containsExactlyInAnyOrderElementsOf(collection);
            // When
            Files.walkFileTree(fileSystem.getPath("/"), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    log.fine(() -> "Deleting [file=" + file + "]...");
                    // When
                    Files.delete(file);
                    // Then
                    assertThat(file).doesNotExist();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (dir.toString().equals("/")) {
                        return FileVisitResult.CONTINUE;
                    }
                    log.fine(() -> "Deleting [dir=" + dir + "]...");
                    // When
                    Files.delete(dir);
                    // Then
                    assertThat(dir).doesNotExist();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // Then
            assertThat(allDirectories(fileSystem, fileSystem.getPath("/"))).containsOnly("/");
        }
    }

}

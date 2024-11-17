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
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.atlantfs.util.AtlantFileUtil.atlantRoot;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.PathUtil.allRegularFiles;
import static org.atlantfs.util.PathUtil.normalize;
import static org.atlantfs.util.PathUtil.projectDir;

@ExtendWith(LoggingExtension.class)
class WriteTest {

    private static final Logger log = Logger.getLogger(WriteTest.class.getName());

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void write_should_persistContent_when_smallContent(TestInfo testInfo) throws IOException {
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
            // When
            Files.writeString(path, text, CREATE);
            var size = Files.size(path);
            // Then
            assertThat(size).isEqualTo(text.length());
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // When
            var actual = Files.readString(path);
            var size = Files.size(path);
            // Then
            assertSoftly(softly -> {
                softly.assertThat(actual).isEqualTo(text);
                softly.assertThat(size).isEqualTo(text.length());
            });
        }
    }

    @Test
    void write_should_persistContent_when_bigContent(TestInfo testInfo) throws IOException {
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
            // When
            Files.writeString(path, text, CREATE);
            var size = Files.size(path);
            // Then
            assertThat(size).isEqualTo(text.length());
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // When
            var actual = Files.readString(path);
            var size = Files.size(path);
            // Then
            assertSoftly(softly -> {
                softly.assertThat(actual).isEqualTo(text);
                softly.assertThat(size).isEqualTo(text.length());
            });
        }
    }

    @Test
    void writeString_should_persistContent_when_bigBigContent(TestInfo testInfo) throws IOException { // File should be > Files::BUFFER_SIZE
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
            // When
            Files.writeString(path, text, CREATE);
            var size = Files.size(path);
            // Then
            assertThat(size).isEqualTo(text.length());
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/LICENSE");
            // When
            var actual = Files.readString(path);
            var size = Files.size(path);
            // Then
            assertSoftly(softly -> {
                softly.assertThat(actual).isEqualTo(text);
                softly.assertThat(size).isEqualTo(text.length());
            });
        }
    }

    @Test
    void write_should_persistContent_when_bigBigContent(TestInfo testInfo) throws IOException { // File should be > Files::BUFFER_SIZE
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        var license = projectDir().resolve("LICENSE");
        var text = Files.readAllBytes(license);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/LICENSE");
            // When
            Files.write(path, text, CREATE);
            var size = Files.size(path);
            // Then
            assertThat(size).isEqualTo(text.length);
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/LICENSE");
            // When
            var actual = Files.readAllBytes(path);
            var size = Files.size(path);
            // Then
            assertSoftly(softly -> {
                softly.assertThat(actual).isEqualTo(text);
                softly.assertThat(size).isEqualTo(text.length);
            });
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
                .numberOfInodeTables(70);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // When
            Files.walkFileTree(projectDir(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (dir.equals(atlantRoot())) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    if (dir.equals(projectDir().resolve("build"))) {
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
                    log.info(() -> "Writing [file=" + file + "]...");
                    var normalized = normalize(file);
                    var bytes = Files.readAllBytes(file);
                    Files.write(fileSystem.getPath(normalized), bytes, CREATE);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            assertThat(allRegularFiles(fileSystem)).containsExactlyInAnyOrderElementsOf(collection);
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // When
            Files.walkFileTree(projectDir(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (dir.equals(atlantRoot())) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    if (dir.equals(projectDir().resolve("build"))) {
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
                    log.info(() -> "Reading [file=" + file + "]...");
                    var normalized = normalize(file);
                    var original = Files.readAllBytes(file);
                    var atlantPath = fileSystem.getPath(normalized);
                    var actual = Files.readAllBytes(atlantPath);
                    // Then
                    assertThat(Files.exists(atlantPath)).isTrue();
                    assertThat(actual).containsExactly(original);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

}

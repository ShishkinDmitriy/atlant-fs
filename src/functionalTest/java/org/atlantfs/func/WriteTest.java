package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;

@ExtendWith(LoggingExtension.class)
class WriteTest {

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

}

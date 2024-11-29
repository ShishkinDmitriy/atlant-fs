package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.atlantfs.util.AtlantFileUtil.atlantRoot;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.PathUtil.allRegularFiles;
import static org.atlantfs.util.PathUtil.normalize;
import static org.atlantfs.util.PathUtil.projectDir;

@ExtendWith(LoggingExtension.class)
class CreateFileTest {

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void createFile_should_createEmptyFile(TestInfo testInfo) throws IOException {
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // When
            Files.createFile(path);
            // Then
            var files = allRegularFiles(fileSystem);
            assertThat(files).containsOnly(path.toString());
        }
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // When
            var exists = Files.exists(path);
            // Then
            assertThat(exists).isTrue();
        }
    }

    @Test
    void createFile_should_throwFileAlreadyExistsException_when_fileAlreadyExists(TestInfo testInfo) throws IOException {
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(64)
                .inodeSize(32)
                .numberOfBlockBitmaps(1)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(1);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var path = fileSystem.getPath("/file.txt");
            // When
            Files.createFile(path);
            // When Then
            assertThatThrownBy(() -> Files.createFile(path))
                    .isInstanceOf(FileAlreadyExistsException.class);
        }
    }

    @Test
    void soapOpera(TestInfo testInfo) throws IOException {
        // Given
        var collection = new ArrayList<String>();
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(128)
                .inodeSize(32)
                .numberOfBlockBitmaps(2)
                .numberOfInodeBitmaps(2)
                .numberOfInodeTables(512);
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // When
            Files.walkFileTree(projectDir(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (dir.equals(atlantRoot())) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    var normalized = normalize(file);
                    var path = fileSystem.getPath(normalized);
                    Files.createFile(path);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            assertThat(allRegularFiles(fileSystem)).containsExactlyInAnyOrderElementsOf(collection);
        }
    }

}

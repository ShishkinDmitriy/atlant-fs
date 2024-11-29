package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.PathUtil.allRegularFiles;
import static org.atlantfs.util.PathUtil.buildDir;
import static org.atlantfs.util.PathUtil.denormalize;
import static org.atlantfs.util.PathUtil.normalize;
import static org.atlantfs.util.PathUtil.projectDir;

@ExtendWith(LoggingExtension.class)
class SoapOperaTest {

    private static final Logger log = Logger.getLogger(SoapOperaTest.class.getName());
    public static final String SUBDIRECTORY = "subdirectory";

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void newFileSystem_should_createNewInstance(TestInfo testInfo) throws IOException {
        // Given
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults()
                .blockSize(512)
                .inodeSize(32)
                .numberOfBlockBitmaps(4)
                .numberOfInodeBitmaps(1)
                .numberOfInodeTables(160);
        // Given
        var collection = new HashSet<String>();
        log.info("Saving project files...");
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
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
                    var normalized = normalize(file);
                    var bytes = Files.readAllBytes(file);
                    var path = fileSystem.getPath(normalized);
                    log.fine(() -> "Writing [file=" + file + "] into [file=" + path + "]...");
                    Files.write(path, bytes, CREATE);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            assertThat(allRegularFiles(fileSystem)).containsExactlyInAnyOrderElementsOf(collection);
        }

        // When
        var deleted = new HashSet<String>();
        log.info("Deleting 70% of all files...");
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            Files.walkFileTree(fileSystem.getPath("/"), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (Math.random() < 0.7) {
                        // When
                        log.fine(() -> "Deleting [file=" + file + "]...");
                        Files.delete(file);
                        deleted.add(normalize(file));
                        // Then
                        assertThat(file).doesNotExist();
                    } else {
                        log.fine(() -> "Do not delete [file=" + file + "]...");
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            assertThat(allRegularFiles(fileSystem)).doesNotContainAnyElementsOf(deleted);
        }

        // When
        var renamed = new HashSet<String>();
        log.info("Saving project files into subdirectory...");
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
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
                    var normalized = addSubdirectory(file);
                    var bytes = Files.readAllBytes(file);
                    var path = fileSystem.getPath(normalized);
                    log.fine(() -> "Writing [file=" + file + "] into [file=" + path + "]...");
                    Files.write(path, bytes, CREATE);
                    collection.add(normalized);
                    return FileVisitResult.CONTINUE;
                }
            });
            // Then
            assertThat(allRegularFiles(fileSystem)).containsAll(renamed);
        }

        // Then
        log.info("Comparing first saved files...");
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // When
            Files.walkFileTree(fileSystem.getPath("/"), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    var subdirPath = fileSystem.getPath(normalize(projectDir())).resolve(SUBDIRECTORY);
                    if (dir.equals(subdirPath)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    var localPath = Paths.get(denormalize(file));
                    var actual = Files.readAllBytes(file);
                    var expected = Files.readAllBytes(localPath);
                    log.fine(() -> "Comparing Atlant [file=" + file + "] vs local [file=" + localPath + "]...");
                    // Then
                    assertThat(actual).containsExactly(expected);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        // Then
        log.info("Comparing second saved files...");
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            var subdirPath = fileSystem.getPath(normalize(projectDir())).resolve(SUBDIRECTORY);
            // When
            Files.walkFileTree(subdirPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    var localPath = Paths.get(removeSubdirectory(file, fileSystem));
                    var actual = Files.readAllBytes(file);
                    var expected = Files.readAllBytes(localPath);
                    log.fine(() -> "Comparing Atlant [file=" + file + "] vs local [file=" + localPath + "]...");
                    // Then
                    assertThat(actual).containsExactly(expected);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    private static String addSubdirectory(Path file) {
        var relative = projectDir().relativize(file);
        var newPath = projectDir().resolve(SUBDIRECTORY).resolve(relative).normalize();
        return normalize(newPath);
    }

    private static String removeSubdirectory(Path file, FileSystem fileSystem) {
        var subdirPath = fileSystem.getPath(normalize(projectDir())).resolve(SUBDIRECTORY);
        var relative = subdirPath.relativize(file);
        var newPath = fileSystem.getPath(normalize(projectDir())).resolve(relative).normalize();
        return denormalize(newPath);
    }

}

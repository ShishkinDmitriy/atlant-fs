package org.atlantfs.util;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

public class PathUtil {

    public static Path projectDir() {
        return Paths.get(System.getProperty("project.dir")); // Defined in build.gradle.kts
    }

    public static Path buildDir() {
        return Paths.get(System.getProperty("build.dir")); // Defined in build.gradle.kts
    }

    public static String normalize(Path path) {
        return path.toString()
                .replaceFirst("C:\\\\", "/")
                .replace('\\', '/');
    }

    public static String denormalize(Path path) {
        return "C:" + path.toAbsolutePath().toString()
                .replace('/', '\\');
    }

    public static ArrayList<String> allRegularFiles(FileSystem fileSystem) throws IOException {
        var files = new ArrayList<String>();
        Files.walkFileTree(fileSystem.getPath("/"), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                files.add(normalize(file));
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }

    public static ArrayList<String> allDirectories(FileSystem fileSystem) throws IOException {
        return allDirectories(fileSystem, fileSystem.getPath(normalize(projectDir())));
    }

    public static ArrayList<String> allDirectories(FileSystem fileSystem, Path path) throws IOException {
        var actual = new ArrayList<String>();
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                actual.add(normalize(dir));
                return FileVisitResult.CONTINUE;
            }
        });
        return actual;
    }

    public static int dirsCount(Path path) throws IOException {
        final int[] count = {0};
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                count[0]++;
                return FileVisitResult.CONTINUE;
            }
        });
        return count[0];
    }

}

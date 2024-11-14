package org.atlantfs.util;

import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class AtlantFileUtil {

    public static Path atlantRoot() {
        return Paths.get(System.getProperty("atlant.dir")); // Defined in build.gradle.kts
    }

    public static Path atlantDir(TestInfo testInfo) {
        var testClass = testInfo.getTestClass().map(Class::getSimpleName).orElse("default");
        return atlantRoot().resolve(testClass);
    }

    public static Path atlantFile(TestInfo testInfo) throws IOException {
        var testMethod = testInfo.getTestMethod().map(Method::getName).orElse("default");
        var parent = atlantDir(testInfo);
        Files.createDirectories(parent);
        return parent.resolve(testMethod + ".atlant");
    }

    public static URI atlantUri(TestInfo testInfo) throws IOException {
        var path = atlantFile(testInfo);
        return URI.create("atlant:" + path.toString().replaceAll("\\\\", "/") + "!/");
    }

    public static void deleteAllAtlantFiles(TestInfo testInfo) throws IOException {
        var dir = atlantDir(testInfo);
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

}

package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;

@ExtendWith(LoggingExtension.class)
class CreateFileSystemTest {

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void createFileSystem_when_noExistingAtlantFile(TestInfo testInfo) throws IOException {
        // When
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults();
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // Then
            assertThat(fileSystem).isNotNull();
            // When
            try (var fileSystem2 = FileSystems.getFileSystem(atlantUri)) {
                // Then
                assertThat(fileSystem2).isNotNull().isSameAs(fileSystem);
            }
        }
        assertThatThrownBy(() -> FileSystems.getFileSystem(atlantUri))
                .isInstanceOf(FileSystemNotFoundException.class);
    }

}

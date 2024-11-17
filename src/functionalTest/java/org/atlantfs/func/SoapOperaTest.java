package org.atlantfs.func;

import org.atlantfs.AtlantConfig;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.assertj.core.api.Assertions.assertThat;
import static org.atlantfs.util.AtlantFileUtil.atlantUri;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;

@ExtendWith(LoggingExtension.class)
class SoapOperaTest {

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void newFileSystem_should_createNewInstance(TestInfo testInfo) throws IOException {
        // When
        var atlantUri = atlantUri(testInfo);
        var atlantConfig = AtlantConfig.defaults();
        try (var fileSystem = FileSystems.newFileSystem(atlantUri, atlantConfig.asMap())) {
            // Then
            assertThat(fileSystem).isNotNull();
            assertThat(fileSystem.isOpen()).isTrue();
            assertThat(fileSystem.isReadOnly()).isFalse();
        }
    }

}

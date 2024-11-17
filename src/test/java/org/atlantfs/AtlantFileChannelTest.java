package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.atlantfs.util.AtlantFileUtil.atlantFile;
import static org.atlantfs.util.AtlantFileUtil.deleteAllAtlantFiles;

@ExtendWith(LoggingExtension.class)
class AtlantFileChannelTest {

    @BeforeAll
    static void beforeEach(TestInfo testInfo) throws IOException {
        deleteAllAtlantFiles(testInfo);
    }

    @Test
    void new_should_throwIllegalArgumentException_ifOpenOptionsIncreased(TestInfo testInfo) throws IOException {
        // Given
        var atlantFile = atlantFile(testInfo);
        Files.createFile(atlantFile);
        try (var _ = AtlantFileChannel.openForRead(atlantFile)) {
            // When Then
            assertThatThrownBy(() -> {
                try (var _ = AtlantFileChannel.openForWrite(atlantFile)) {
                    AtlantFileChannel.get();
                }
            }).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void get_should_beOpenInside(TestInfo testInfo) throws IOException {
        // Given
        SeekableByteChannel channel;
        var atlantFile = atlantFile(testInfo);
        try (var _ = AtlantFileChannel.openForCreate(atlantFile)) {
            // When
            channel = AtlantFileChannel.get();
            // Then
            assertThat(channel).isNotNull();
            assertThat(channel.isOpen()).isTrue();
        }
        // Then
        assertThat(channel).isNotNull();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void get_should_notBeClosedIfReentrant(TestInfo testInfo) throws IOException {
        // Given
        SeekableByteChannel channel0;
        var atlantFile = atlantFile(testInfo);
        try (var _ = AtlantFileChannel.openForCreate(atlantFile)) {
            // When
            channel0 = AtlantFileChannel.get();
            try (var _ = AtlantFileChannel.openForRead(atlantFile)) {
                // When
                var channel1 = AtlantFileChannel.get();
                try (var _ = AtlantFileChannel.openForRead(atlantFile)) {
                    // When
                    var channel2 = AtlantFileChannel.get();
                    // Then
                    assertThat(channel2).isNotNull();
                    assertSoftly(softly -> {
                        softly.assertThat(channel2).isSameAs(channel1);
                        softly.assertThat(channel2).isSameAs(channel0);
                        softly.assertThat(channel2.isOpen()).isTrue();
                    });
                }
                // Then
                assertThat(channel1).isNotNull();
                assertSoftly(softly -> {
                    softly.assertThat(channel1).isSameAs(channel0);
                    softly.assertThat(channel1.isOpen()).isTrue();
                });
            }
            // Then
            assertThat(channel0).isNotNull();
            assertThat(channel0.isOpen()).isTrue();
        }
        // Then
        assertThat(channel0).isNotNull();
        assertThat(channel0.isOpen()).isFalse();
    }

}
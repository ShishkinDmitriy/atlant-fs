package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

@ExtendWith({LoggingExtension.class, MockitoExtension.class})
class AtlantChannelTest {

    private static final String ATLANT_FILE_NAME = "build/AtlantChannelTest.atlant";
    private static final Path ATLANT_FILE = Paths.get(ATLANT_FILE_NAME);

    @BeforeEach
    void beforeEach() throws IOException {
        if (!Files.exists(ATLANT_FILE)) {
            Files.createFile(ATLANT_FILE);
        }
    }

    @Test
    void new_should_throwIllegalArgumentException_ifOpenOptionsIncreased(@Mock OpenOption option) throws IOException {
        // Given
        try (var _ = new AtlantChannel(ATLANT_FILE, READ)) {
            // When Then
            assertThatThrownBy(() -> {
                try (var _ = new AtlantChannel(ATLANT_FILE, READ, option)) {
                    AtlantChannel.get();
                }
            }).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void get_should_beOpenInside() throws IOException {
        // Given
        SeekableByteChannel channel;
        try (var _ = new AtlantChannel(ATLANT_FILE, READ)) {
            // When
            channel = AtlantChannel.get();
            // Then
            assertThat(channel).isNotNull();
            assertThat(channel.isOpen()).isTrue();
        }
        // Then
        assertThat(channel).isNotNull();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void get_should_notBeClosedIfReentrant() throws IOException {
        // Given
        SeekableByteChannel channel0;
        try (var _ = new AtlantChannel(ATLANT_FILE, READ)) {
            // When
            channel0 = AtlantChannel.get();
            try (var _ = new AtlantChannel(ATLANT_FILE, READ)) {
                // When
                var channel1 = AtlantChannel.get();
                try (var _ = new AtlantChannel(ATLANT_FILE, READ)) {
                    // When
                    var channel2 = AtlantChannel.get();
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
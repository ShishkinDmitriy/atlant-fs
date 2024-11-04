package org.atlantfs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

// Hex prepared in https://hexed.it/
class DirEntryTest {

    private final Random random = new Random();

    @Test
    void read_shouldParseDirEntry() {
        // Given
        ByteBuffer buffer = byteBuffer("""
                40E2 0100 1000 0301 6469 7200 0000 0000\
                """);
        // When
        DirEntry result = DirEntry.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getPosition()).isEqualTo(0);
            softly.assertThat(result.getInode()).isEqualTo(123456);
            softly.assertThat(result.getLength()).isEqualTo((short) 16);
            softly.assertThat(result.getFileType()).isEqualTo(FileType.REGULAR_FILE);
            softly.assertThat(result.getName()).isEqualTo("dir");
            softly.assertThat(result.isEmpty()).isFalse();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_shouldParseDirEntry_whenExpandedEntry() {
        // Given
        ByteBuffer buffer = byteBuffer("""
                0000 0000 2000 0302 6469 7200 0000 0000\
                F1FB 0900 1000 0402 6469 7232 0000 0000\
                """);
        // When
        DirEntry result = DirEntry.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getPosition()).isEqualTo(0);
            softly.assertThat(result.getInode()).isEqualTo(0);
            softly.assertThat(result.getLength()).isEqualTo((short) 32);
            softly.assertThat(result.getFileType()).isEqualTo(FileType.UNKNOWN);
            softly.assertThat(result.getName()).isEqualTo("");
            softly.assertThat(result.isEmpty()).isTrue();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_shouldParseDirEntry_whenReadFromMiddle() {
        // Given
        ByteBuffer buffer = byteBuffer("""
                40E2 0100 1000 0302 6469 7200 0000 0000\
                F1FB 0900 1000 0402 6469 7232 0000 0000\
                """);
        buffer.position(16);
        // When
        DirEntry result = DirEntry.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getPosition()).isEqualTo(16);
            softly.assertThat(result.getInode()).isEqualTo(654321);
            softly.assertThat(result.getLength()).isEqualTo((short) 16);
            softly.assertThat(result.getFileType()).isEqualTo(FileType.DIRECTORY);
            softly.assertThat(result.getName()).isEqualTo("dir2");
            softly.assertThat(result.isEmpty()).isFalse();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_shouldThrowIllegalArgumentException_when() {
        // Given
        ByteBuffer buffer = byteBuffer("""
                0000 0000 0000 0000 0000 0000 0000 0000\
                0000 0000 0000 0000 0000 0000 0000 0000\
                """);
        // When Then
        assertThatThrownBy(() -> DirEntry.read(buffer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Too small Dir entry");
    }

    @ParameterizedTest
    @CsvSource({
            "  1, 7",
            "  2, 6",
            "  8, 0",
            "  9, 7",
            "254, 2",
            "255, 1",
    })
    void calculateMinPadding_shouldReturnPadding(int nameLength, int expectedMinPadding) {
        // Given
        DirEntry entry = new DirEntry(0);
        entry.setName(randomString(nameLength));
        // When
        int result = entry.calculateMinPadding();
        // Then
        assertThat(result).isEqualTo(expectedMinPadding);
    }

    private String randomString(int nameLength) {
        return random.ints('a', 'z')
                .limit(nameLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private static ByteBuffer byteBuffer(String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex.replaceAll(" ", ""));
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

}

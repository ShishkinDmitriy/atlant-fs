package org.atlantfs;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.atlantfs.util.ByteBufferUtil.byteBuffer;
import static org.atlantfs.util.RandomUtil.randomInt;
import static org.atlantfs.util.RandomUtil.randomString;

// Hex prepared in https://hexed.it/
class DirEntryTest {

    @Nested
    class Read {

        @Test
        void should_parseDirEntry() {
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
                softly.assertThat(result.getInode()).isEqualTo(new Inode.Id(123456));
                softly.assertThat(result.getLength()).isEqualTo((short) 16);
                softly.assertThat(result.getFileType()).isEqualTo(FileType.REGULAR_FILE);
                softly.assertThat(result.getName()).isEqualTo("dir");
                softly.assertThat(result.isEmpty()).isFalse();
            });
            assertThat(buffer.hasRemaining()).isFalse();
        }

        @Test
        void should_parseDirEntry_when_expandedEntry() {
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
                softly.assertThat(result.getInode()).isEqualTo(Inode.Id.NULL);
                softly.assertThat(result.getLength()).isEqualTo((short) 32);
                softly.assertThat(result.getFileType()).isEqualTo(FileType.UNKNOWN);
                softly.assertThat(result.getName()).isEqualTo("");
                softly.assertThat(result.isEmpty()).isTrue();
            });
            assertThat(buffer.hasRemaining()).isFalse();
        }

        @Test
        void should_parseDirEntry_when_readFromMiddle() {
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
                softly.assertThat(result.getInode()).isEqualTo(new Inode.Id(654321));
                softly.assertThat(result.getLength()).isEqualTo((short) 16);
                softly.assertThat(result.getFileType()).isEqualTo(FileType.DIRECTORY);
                softly.assertThat(result.getName()).isEqualTo("dir2");
                softly.assertThat(result.isEmpty()).isFalse();
            });
            assertThat(buffer.hasRemaining()).isFalse();
        }

        @Test
        void should_throwIllegalArgumentException_when_allZero() {
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

    }

    @Nested
    class Aligned {

        @ParameterizedTest
        @CsvSource({
                "  1,  16",
                "  2,  16",
                "  8,  16",
                "  9,  24",
                "254, 264",
                "255, 264",
        })
        void should_returnAlignedValue(int nameLength, int expectedMinPadding) {
            // When
            var result = DirEntry.aligned((short) nameLength);
            // Then
            assertThat(result).isEqualTo((short) expectedMinPadding);
        }

    }

    @Nested
    class Split {

        @ParameterizedTest
        @CsvSource({
                // Initial          | Another | Expected        |
                // pos | len | name | name    | len | pos | len |
                "     0, 4096,     1,        1,   16,   16, 4080",
                "     0, 4096,   255,        1,  264,  264, 3832",
                "     0, 4096,     1,      255,   16,   16, 4080",
                "   128,  528,   255,      255,  264,  392,  264",
        })
        void should_returnNewEntry(short position, short length, short nameLength, short anotherNameLength, short expectedLength, short expectedAnotherPosition, short expectedAnotherLength) {
            // Given
            var entry = DirEntry.create(position, length, new Inode.Id(randomInt()), FileType.REGULAR_FILE, randomString(nameLength));
            // When
            var result = entry.split(new Inode.Id(randomInt()), FileType.REGULAR_FILE, randomString(anotherNameLength));
            // Then
            assertSoftly(softly -> {
                softly.assertThat(entry.isDirty()).describedAs("Initial entry should marked as dirty").isTrue();
                softly.assertThat(entry.getPosition()).describedAs("Initial entry should not moved").isEqualTo(position);
                softly.assertThat(entry.getLength()).describedAs("Initial entry should shrink as expected").isEqualTo(expectedLength);
            });
            assertThat(result).isNotNull();
            assertSoftly(softly -> {
                softly.assertThat(result.isDirty()).describedAs("New entry should marked as dirty").isTrue();
                softly.assertThat(result.getPosition()).isEqualTo(expectedAnotherPosition);
                softly.assertThat(result.getLength()).isEqualTo(expectedAnotherLength);
            });
            assertThat(entry.getLength() + result.getLength()).describedAs("Sum length should not be changed").isEqualTo(length);
        }

        @Test
        void should_mutateExistingEntry_when_emptyEntry() {
            // Given
            short length = (short) 4096;
            var entry = DirEntry.empty(length);
            Inode.Id inode = new Inode.Id(randomInt());
            String name = randomString(255);
            // When
            var result = entry.split(inode, FileType.REGULAR_FILE, name);
            // Then
            assertThat(result).isNotNull();
            assertThat(result).describedAs("Should return same entry").isSameAs(entry);
            assertSoftly(softly -> {
                softly.assertThat(entry.getPosition()).describedAs("Entry should has the same position").isEqualTo(0);
                softly.assertThat(entry.getLength()).describedAs("Entry should has the same length").isEqualTo(length);
                softly.assertThat(entry.getName()).isEqualTo(name);
                softly.assertThat(entry.getInode()).isEqualTo(inode);
                softly.assertThat(entry.isDirty()).isTrue();
            });
        }

    }

    @Nested
    class CanBeSplit {

        @ParameterizedTest
        @CsvSource({
                // Initial    | Another | Expected |
                // len | name | name    | result   |
                "    16,     1,        1,    false",
                "    24,     1,        1,    false",
                "    32,     1,        1,     true",
                "    32,     8,        1,     true",
                "    32,     1,        8,     true",
                "    32,     1,        9,    false",
                "  4096,     1,        1,     true",
                "  4096,   255,        1,     true",
                "  4096,     1,      255,     true",
                "   528,   255,      255,     true", // 528 = 264 + 264
        })
        void should_returnTrueOrFalse(short length, short nameLength, short anotherNameLength, boolean expectedResult) {
            // Given
            var entry = DirEntry.create(randomInt(), length, new Inode.Id(randomInt()), FileType.REGULAR_FILE, randomString(nameLength));
            // When
            var result = entry.canBeSplit(anotherNameLength);
            // Then
            assertThat(result).isEqualTo(expectedResult);
        }

    }

    @Nested
    class Rename {

        @ParameterizedTest
        @CsvSource({
                // Initial    |  New | Expected |
                // len | name | name | renamed? |
                "    16,     1,     1,      true",
                "    16,     1,     8,      true",
                "    16,     1,     9,     false",
                "    16,     8,     1,      true",
                "    16,     8,     8,      true",
                "    16,     8,     9,     false",
                "  1024,     1,   255,      true",
        })
        void should_changeName_whenEnoughSpace(short length, short nameLength, short newNameLength, boolean expectedResult) {
            // Given
            String name = randomString(nameLength);
            String newName = randomString(newNameLength);
            var entry = DirEntry.create(randomInt(), length, new Inode.Id(randomInt()), FileType.REGULAR_FILE, name);
            // When
            boolean result = entry.rename(newName);
            // Then
            assertThat(result).isEqualTo(expectedResult);
            SoftAssertions.assertSoftly(softly -> {
                if (result) {
                    softly.assertThat(entry.getName()).isEqualTo(newName);
                    softly.assertThat(entry.isDirty()).isTrue();
                } else {
                    softly.assertThat(entry.getName()).isEqualTo(name);
                    softly.assertThat(entry.isDirty()).isFalse();
                }
            });
        }

    }

}
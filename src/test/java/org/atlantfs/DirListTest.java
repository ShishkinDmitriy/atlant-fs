package org.atlantfs;

import org.atlantfs.util.CommaSeparatedListConverter;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.atlantfs.util.ByteBufferUtil.byteBuffer;
import static org.atlantfs.util.RandomUtil.randomInt;
import static org.atlantfs.util.RandomUtil.randomString;

@ExtendWith(LoggingExtension.class)
class DirListTest {

    //region DirList::read
    @Test
    void read_should_parseDirEntries_when_onlyOneGoodEntry() {
        // Given
        var buffer = byteBuffer("""
                40E2 0100 1000 0301 6469 7200 0000 0000\
                """);
        // When
        var result = DirList.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getEntries()).isNotNull().hasSize(1);
            softly.assertThat(result.isDirty()).isFalse();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_should_parseDirEntries_when_onlyOneExpandedEntry() {
        // Given
        var buffer = byteBuffer("""
                0000 0000 2000 0302 6469 7200 0000 0000\
                F1FB 0900 1000 0402 6469 7232 0000 0000\
                """);
        // When
        var result = DirList.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getEntries()).isNotNull().hasSize(1);
            softly.assertThat(result.isDirty()).isFalse();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_should_prepareBrokenEntry_when_lastEntryHasWrongSize() {
        // Given
        var buffer = byteBuffer("""
                0000 0000 1000 0302 6469 7200 0000 0000\
                F1FB 0900 2000 0402 6469 7232 0000 0000\
                """);
        // When
        var result = DirList.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getEntries()).isNotNull().hasSize(1)
                    .extracting(DirEntry::isDirty).containsExactly(true);
            softly.assertThat(result.isDirty()).isTrue();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void read_should_addUnknownEntry_when_allZero() {
        // Given
        var buffer = byteBuffer("""
                0000 0000 0000 0000 0000 0000 0000 0000\
                0000 0000 0000 0000 0000 0000 0000 0000\
                """);
        // When
        var result = DirList.read(buffer);
        // Then
        assertThat(result).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(result.getEntries()).isNotNull().hasSize(1)
                    .extracting(DirEntry::isDirty).containsExactly(true);
            softly.assertThat(result.isDirty()).isTrue();
        });
        assertThat(buffer.hasRemaining()).isFalse();
    }
    //endregion

    //region DirList::delete
    @ParameterizedTest
    @CsvSource(value = {
            // Initial                     | Expected                   |
            // Lengths     | Names   | Del | Lengths    | Names | Dirty |
            "16,24,64,1024 | a,b,c,d | a   | 40,64,1024 | b,c,d | 1,0,0 ",
            "16,24,64,1024 | a,b,c,d | b   | 40,64,1024 | a,c,d | 1,0,0 ",
            "16,24,64,1024 | a,b,c,d | c   | 16,88,1024 | a,b,d | 0,1,0 ",
            "16,24,64,1024 | a,b,c,d | d   | 16,24,1088 | a,b,c | 0,0,1 ",
    }, delimiter = '|')
    void delete_should_expandNeighbourEntry_when_sizeIsMoreThan1(
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> lengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> names,
            String nameToDelete,
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> expectedLengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> expectedNames,
            @ConvertWith(CommaSeparatedListConverter.class) List<Boolean> expectedDirty) throws NoSuchFileException {
        // Given
        List<DirEntry> entries = new ArrayList<>();
        var pos = 0;
        for (int i = 0; i < lengths.size(); i++) {
            entries.add(DirEntry.create(pos, lengths.get(i), Inode.Id.of(randomInt()), FileType.DIRECTORY, names.get(i)));
            pos += lengths.get(i);
        }
        var block = new DirList(pos, entries);
        // When
        block.delete(nameToDelete);
        // Then
        assertSoftly(softly -> {
            softly.assertThat(block.getEntries()).extracting(DirEntry::getLength).containsExactlyElementsOf(expectedLengths);
            softly.assertThat(block.getEntries()).extracting(DirEntry::getName).containsExactlyElementsOf(expectedNames);
            softly.assertThat(block.getEntries()).extracting(DirEntry::isDirty).containsExactlyElementsOf(expectedDirty);
            softly.assertThat(block.isDirty()).isTrue();
        });
    }

    @Test
    void delete_should_deleteDirEntry_when_sizeIs1() throws NoSuchFileException {
        // Given
        var position = 0;
        var length = (short) 1024;
        var name = randomString(255);
        var entry = DirEntry.create(position, length, Inode.Id.of(123), FileType.DIRECTORY, name);
        var block = new DirList(length, List.of(entry));
        // When
        block.delete(name);
        // Then
        assertSoftly(softly -> {
            softly.assertThat(block.getEntries()).hasSize(1);
            softly.assertThat(block.isDirty()).isTrue();
        });
        var deleted = block.getEntries().getFirst();
        assertSoftly(softly -> {
            softly.assertThat(deleted).isNotNull();
            softly.assertThat(deleted.getPosition()).isEqualTo(position);
            softly.assertThat(deleted.getLength()).isEqualTo(length);
            softly.assertThat(deleted.isDirty()).isTrue();
            softly.assertThat(deleted.isEmpty()).isTrue();
            softly.assertThat(deleted.getInode()).isEqualTo(Inode.Id.NULL);
            softly.assertThat(deleted.getName()).isEmpty();
        });
    }
    //endregion

    //region DirList::add
    @ParameterizedTest
    @CsvSource(value = {
            // Initial                           | Expected                                         |
            // Lengths     | Names   | Add       | Lengths          | Names             | Dirty     |
            "16,32,64,1024 | a,b,c,d | Z         | 16,16,16,64,1024 | a,b,Z,c,d         | 0,1,1,0,0 ",
            "16,32,64,1024 | a,b,c,d | Z12345678 | 16,32,16,48,1024 | a,b,c,Z12345678,d | 0,0,1,1,0 ",
    }, delimiter = '|')
    void add_should_splitEntryWithAvailableSpace(
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> lengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> names,
            String nameToAdd,
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> expectedLengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> expectedNames,
            @ConvertWith(CommaSeparatedListConverter.class) List<Boolean> expectedDirty) throws DirList.NotEnoughSpaceException {
        // Given
        List<DirEntry> entries = new ArrayList<>();
        var pos = 0;
        for (int i = 0; i < lengths.size(); i++) {
            entries.add(DirEntry.create(pos, lengths.get(i), Inode.Id.of(randomInt()), FileType.DIRECTORY, names.get(i)));
            pos += lengths.get(i);
        }
        var block = new DirList(pos, entries);
        // When
        var result = block.add(Inode.Id.of(randomInt()), FileType.REGULAR_FILE, nameToAdd);
        // Then
        assertThat(result).isNotNull();
        assertThat(result.isDirty()).isTrue();
        assertSoftly(softly -> {
            softly.assertThat(block.getEntries()).extracting(DirEntry::getLength).containsExactlyElementsOf(expectedLengths);
            softly.assertThat(block.getEntries()).extracting(DirEntry::getName).containsExactlyElementsOf(expectedNames);
            softly.assertThat(block.getEntries()).extracting(DirEntry::isDirty).containsExactlyElementsOf(expectedDirty);
            softly.assertThat(block.isDirty()).isTrue();
        });
    }

    @Test
    void should_mutateExistingEntry_when_emptyEntry() throws DirList.NotEnoughSpaceException {
        // Given
        short length = (short) 4096;
        Inode.Id inode = Inode.Id.of(randomInt());
        String name = randomString(255);
        var block = new DirList(length);
        // When
        var result = block.add(inode, FileType.DIRECTORY, name);
        // Then
        assertThat(result).isNotNull();
        assertThat(block.getEntries()).isNotNull().hasSize(1);
        var entry = block.getEntries().getFirst();
        assertSoftly(softly -> {
            softly.assertThat(entry.getPosition()).describedAs("Entry should has the same position").isEqualTo(0);
            softly.assertThat(entry.getLength()).describedAs("Entry should has the same length").isEqualTo(length);
            softly.assertThat(entry.getName()).isEqualTo(name);
            softly.assertThat(entry.getInode()).isEqualTo(inode);
            softly.assertThat(entry.isDirty()).isTrue();
        });
    }
    //endregion

    //region DirList::rename
    @ParameterizedTest
    @CsvSource(value = {
            // Initial                                    | Expected                          |
            // Lengths     | Names   | Rename | Rename len| Lengths       | Names   | Dirty   |
            "16,32,64,1024 | a,b,c,d | a      |         1 | 16,32,64,1024 | *,b,c,d | 1,0,0,0 ", // asterisk (*) means new name
            "16,32,64,1024 | a,b,c,d | a      |         8 | 16,32,64,1024 | *,b,c,d | 1,0,0,0 ",
            "16,32,64,1024 | a,b,c,d | a      |         9 | 16,32,64,1024 | b,*,c,d | 1,1,0,0 ", // Entry a deleted, b grow left and become 48 then split b => a and b swapped
            "16,32,64,1024 | a,b,c,d | a      |       255 | 48,64,16,1008 | b,c,d,* | 1,0,1,1 ",
            "16,32,64,1024 | a,b,c,d | c      |         1 | 16,32,64,1024 | a,b,*,d | 0,0,1,0 ",
            "16,32,64,1024 | a,b,c,d | c      |       255 | 16,96,16,1008 | a,b,d,* | 0,1,1,1 ", // Entry c deleted, b grow right and become 96 then split d
            "16,32,64,1024 | a,b,c,d | d      |         1 | 16,32,64,1024 | a,b,c,* | 0,0,0,1 ",
            "16,32,64,1024 | a,b,c,d | d      |       255 | 16,32,64,1024 | a,b,c,* | 0,0,0,1 ",
            "  16,16,16,16 | a,b,c,d | a      |         1 |   16,16,16,16 | *,b,c,d | 1,0,0,0 ",
            "  16,16,16,16 | a,b,c,d | a      |         8 |   16,16,16,16 | *,b,c,d | 1,0,0,0 ",
            "  16,32,16,16 | a,b,c,d | a      |         9 |   16,32,16,16 | b,*,c,d | 1,1,,00 ", // Entry a deleted, b grow left and become 32 then split b
    }, delimiter = '|')
    void rename_should_renameInPlaceOrAllocateNewPlace(
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> lengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> names,
            String oldName,
            int newNameLength,
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> expectedLengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> expectedNames,
            @ConvertWith(CommaSeparatedListConverter.class) List<Boolean> expectedDirty) throws NoSuchFileException, DirList.NotEnoughSpaceException {
        // Given
        List<DirEntry> entries = new ArrayList<>();
        var pos = 0;
        for (int i = 0; i < lengths.size(); i++) {
            entries.add(DirEntry.create(pos, lengths.get(i), Inode.Id.of(randomInt()), FileType.DIRECTORY, names.get(i)));
            pos += lengths.get(i);
        }
        var block = new DirList(pos, entries);
        var newName = randomString(newNameLength);
        // When
        block.rename(oldName, newName);
        // Then
        assertSoftly(softly -> {
            softly.assertThat(block.getEntries()).extracting(DirEntry::getLength).containsExactlyElementsOf(expectedLengths);
            softly.assertThat(block.getEntries()).extracting(DirEntry::getName).containsExactlyElementsOf(expectedNames.stream().map(s -> s.equals("*") ? newName : s).toList());
            softly.assertThat(block.getEntries()).extracting(DirEntry::isDirty).containsExactlyElementsOf(expectedDirty);
            softly.assertThat(block.isDirty()).isTrue();
        });
    }

    @ParameterizedTest
    @CsvSource(value = {
            // Lengths     | Names   | Rename | Rename len|
            "  16,16,16,16 | a,b,c,d | a      |         9 ",
            "  16,16,32,16 | a,b,c,d | a      |         9 ", // After delete both a and c can split for 8, but required 16
            "  32,16,16,16 | a,b,c,d | c      |         9 ",
            "  32,16,16,16 | a,b,c,d | d      |         9 ",
    }, delimiter = '|')
    void rename_should_throwIllegalStateException_when_notEnoughSpaceToRelocate(
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> lengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> names,
            String oldName,
            int newNameLength) {
        // Given
        List<DirEntry> entries = new ArrayList<>();
        var pos = 0;
        for (int i = 0; i < lengths.size(); i++) {
            entries.add(DirEntry.create(pos, lengths.get(i), Inode.Id.of(randomInt()), FileType.DIRECTORY, names.get(i)));
            pos += lengths.get(i);
        }
        var block = new DirList(pos, entries);
        String newName = randomString(newNameLength);
        // When Then
        assertThatThrownBy(() -> block.rename(oldName, newName))
                .isInstanceOf(DirList.NotEnoughSpaceException.class)
                .hasMessageContaining("Not enough space");
    }
    //endregion

}

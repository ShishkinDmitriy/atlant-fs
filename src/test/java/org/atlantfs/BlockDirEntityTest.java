package org.atlantfs;

import org.atlantfs.util.CommaSeparatedListConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.NoSuchFileException;
import java.util.List;

import static org.assertj.core.api.SoftAssertions.assertSoftly;

class BlockDirEntityTest {

    @ParameterizedTest
    @CsvSource(value = {
            // Sizes       | Empty?  | Names   | Del | Expected lengths | Expected empty|
            "10,20,40,1024 | 0,0,0,0 | a,b,c,d | a   | 10,20,40,1024    | 1,0,0,0  ",
            "10,20,40,1024 | 0,0,0,0 | a,b,c,d | b   | 30,40,1024       | 0,0,0    ",
            "10,20,40,1024 | 0,0,0,0 | a,b,c,d | c   | 10,60,1024       | 0,0,0    ",
            "10,20,40,1024 | 0,0,0,0 | a,b,c,d | d   | 10,20,1064       | 0,0,0    ",

            "10,20,40,1024 | 1,0,0,0 | a,b,c,d | b   | 30,40,1024       | 1,0,0    ",
            "10,20,40,1024 | 1,0,0,0 | a,b,c,d | c   | 10,60,1024       | 1,0,0    ",
            "10,20,40,1024 | 1,0,0,0 | a,b,c,d | d   | 10,20,1064       | 1,0,0    ",

            "10,20,40,1024 | 0,1,0,0 | a,b,c,d | a   | 30,40,1024       | 1,0,0    ",
            "10,20,40,1024 | 0,1,0,0 | a,b,c,d | c   | 10,60,1024       | 0,1,0    ",
            "10,20,40,1024 | 0,1,0,0 | a,b,c,d | d   | 10,20,1064       | 0,1,0    ",

            "10,20,40,1024 | 0,0,0,1 | a,b,c,d | a   | 10,20,40,1024    | 1,0,0,1  ",
            "10,20,40,1024 | 0,0,0,1 | a,b,c,d | b   | 30,40,1024       | 0,0,1    ",
            "10,20,40,1024 | 0,0,0,1 | a,b,c,d | c   | 10,1084          | 0,0      ",
            "1024          | 0       | a,b,c,d | c   | 10,1084          | 0,0      ",
    }, delimiter = '|')
    void delete_shouldExpandDirEntry(
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> lengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<Boolean> empty,
            @ConvertWith(CommaSeparatedListConverter.class) List<String> names,
            String nameToDelete,
            @ConvertWith(CommaSeparatedListConverter.class) List<Short> expectedLengths,
            @ConvertWith(CommaSeparatedListConverter.class) List<Boolean> expectedEmpty) throws NoSuchFileException {
        // Given
        BlockDirEntity block = new BlockDirEntity();
        block.getEntries().clear();
        int size = lengths.size();
        int pos = 0;
        for (int i = 0; i < size; i++) {
            DirEntry entry = new DirEntry(pos);
            entry.setLength(lengths.get(i));
            entry.setName(names.get(i));
            entry.setInode(empty.get(i) ? Inode.NULL : 123);
            block.getEntries().add(entry);
            pos += lengths.get(i);
        }
        // When
        block.delete(nameToDelete);
        // Then
        assertSoftly(softly -> {
            softly.assertThat(block.getEntries()).extracting(DirEntry::getLength).containsExactlyElementsOf(expectedLengths);
            softly.assertThat(block.getEntries()).extracting(DirEntry::isEmpty).containsExactlyElementsOf(expectedEmpty);
        });
    }

}
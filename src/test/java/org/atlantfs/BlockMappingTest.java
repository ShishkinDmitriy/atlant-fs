package org.atlantfs;

import org.atlantfs.util.CommaSeparatedListConverter;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, LoggingExtension.class})
class BlockMappingTest {

    @CsvSource(value = {
            // Given      | Call         | Expected |
            // block size | block number |    level |
            "           8 |            0 |        1 ", // 8 bytes -> 2 ids per block
            "           8 |            1 |        1 ",
            "           8 |            2 |        2 ",
            "           8 |            5 |        2 ",
            "           8 |            6 |        3 ",
            "         512 |            0 |        1 ", // 512 bytes -> 128 ids per block
            "         512 |          127 |        1 ",
            "         512 |          128 |        2 ",
            "         512 |        16384 |        2 ", // 16_384 == 128 * 128
            "         512 |        16511 |        2 ", // 16_511 == 128 * 128 + 128
            "         512 |        16512 |        3 ",
            "        4096 |   1074791423 |        3 ", // 1,074,791,424 == 1024^3 + 1024^2 + 1024 - 1
            "        4096 |   1074791424 |        4 ", // 1,074,791,424 == 1024^3 + 1024^2 + 1024
            "        4096 |   4294967296 |        5 ", // 4_294_967_296 == 2^32
    }, delimiter = '|')
    @ParameterizedTest
    void indirectLevel_should_calculateIndirectOffsets(int blockSize, long blockNumber, int expectedLevel, @Mock Inode inode) {
        // Given
        when(inode.blockSize()).thenReturn(blockSize);
        var blockList = new BlockMapping(inode);
        // When
        var result = blockList.indirectLevel(blockNumber);
        // Then
        assertThat(result).isEqualTo(expectedLevel);
    }

    @CsvSource(value = {
            // Given      | Call                 | Expected |
            // block size | level | block number | result   |
            "           8 |     1 |            0 |        0 ", // 8 bytes -> 2 ids per block
            "           8 |     1 |            1 |        1 ",
            "           8 |     2 |            0 |      0,0 ",
            "           8 |     2 |            1 |      0,1 ",
            "           8 |     2 |            2 |      1,0 ",
            "           8 |     2 |            3 |      1,1 ",
            "           8 |     3 |            0 |    0,0,0 ",
            "           8 |     3 |            1 |    0,0,1 ",
            "           8 |     3 |            2 |    0,1,0 ",
            "           8 |     3 |            3 |    0,1,1 ",
            "           8 |     3 |            4 |    1,0,0 ",
            "           8 |     3 |            5 |    1,0,1 ",
            "           8 |     3 |            6 |    1,1,0 ",
            "           8 |     3 |            7 |    1,1,1 ",
            "         512 |     1 |            0 |        0 ", // 512 bytes -> 128 ids per block
            "         512 |     1 |          127 |      127 ",
            "         512 |     2 |            0 |      0,0 ",
            "         512 |     2 |          127 |    0,127 ",
            "         512 |     2 |          128 |      1,0 ",
            "         512 |     2 |        16383 |  127,127 ",
            "         512 |     3 |            0 |    0,0,0 ",
            "         512 |     3 |        16384 |    1,0,0 ", // 16_384 == 128 * 128
    }, delimiter = '|')
    @ParameterizedTest
    void indirectOffsets_should_calculateIndirectOffsets(int blockSize, int level, int blockNumber, @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedCoordinates, @Mock Inode inode) {
        // Given
        when(inode.blockSize()).thenReturn(blockSize);
        var blockList = new BlockMapping(inode);
        // When
        var result = blockList.indirectOffsets(level, blockNumber);
        // Then
        assertThat(result).containsExactlyElementsOf(expectedCoordinates);
    }

}

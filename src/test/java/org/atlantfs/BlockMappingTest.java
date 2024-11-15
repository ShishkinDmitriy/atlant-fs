package org.atlantfs;

import org.atlantfs.util.CommaSeparatedListConverter;
import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith({LoggingExtension.class, MockitoExtension.class})
class BlockMappingTest {

    //region BlockMapping::addressLevel
    @CsvSource(value = {
            // Call                                   | Expected |
            // block size | inode size | block number |   result |
            "          32 |         32 |            0 |        0 ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block
            "          32 |         32 |            1 |        1 ", // 1
            "          32 |         32 |            2 |        1 ",
            "          32 |         32 |            3 |        1 ",
            "          32 |         32 |            9 |        2 ", // 1 + 8
            "          32 |         32 |           10 |        2 ",
            "          32 |         32 |           73 |        3 ", // 1 + 8 + 8^2
            "          32 |         32 |           74 |        3 ",
            "          32 |         32 |          585 |        4 ", // 1 + 8 + 8^2 + 8^3

            "          64 |         64 |            0 |        0 ", // 48 bytes for iblock, 9 direct + 3 indirect, 16 ids per block
            "          64 |         64 |            1 |        0 ",
            "          64 |         64 |            9 |        1 ", // 9
            "          64 |         64 |           10 |        1 ",
            "          64 |         64 |           25 |        2 ", // 9 + 16
            "          64 |         64 |           26 |        2 ",
            "          64 |         64 |          281 |        3 ", // 9 + 16 + 16^2
            "          64 |         64 |          282 |        3 ",
            "          64 |         64 |         4377 |        4 ", // 9 + 16 + 16^2 + 16^3

            "         128 |         64 |            0 |        0 ", // 48 bytes for iblock, 9 direct + 3 indirect, 32 ids per block
            "         128 |         64 |            1 |        0 ",
            "         128 |         64 |            9 |        1 ", // 9
            "         128 |         64 |           10 |        1 ",
            "         128 |         64 |           41 |        2 ", // 9 + 32
            "         128 |         64 |           42 |        2 ",
            "         128 |         64 |         1065 |        3 ", // 9 + 32 + 32^2
            "         128 |         64 |         1066 |        3 ",
            "         128 |         64 |        33833 |        4 ", // 9 + 32 + 32^2 + 32^3

            "        4096 |        128 |            0 |        0 ", // 48 bytes for iblock, 25 direct + 3 indirect, 1024 ids per block
            "        4096 |        128 |            1 |        0 ",
            "        4096 |        128 |           25 |        1 ", // 25
            "        4096 |        128 |           26 |        1 ",
            "        4096 |        128 |           27 |        1 ",
            "        4096 |        128 |         1049 |        2 ", // 25 + 1024
            "        4096 |        128 |         1050 |        2 ",
            "        4096 |        128 |      1049625 |        3 ", // 25 + 1024 + 1024^2
            "        4096 |        128 |      1049626 |        3 ",
            "        4096 |        128 |   1074791449 |        4 ", // 25 + 1024 + 1024^2 + 1024^3
    }, delimiter = '|')
    @ParameterizedTest
    void addressLevel_should_calculateNumber(int blockSize, int inodeSize, long blockNumber, int expectedLevel) {
        // When
        var result = BlockMapping.addressLevel(blockSize, inodeSize, blockNumber);
        // Then
        assertThat(result).isEqualTo(expectedLevel);
    }
    //endregion

    //region BlockMapping::addressOffsets
    @CsvSource(value = {
            // Call                                   |    Expected |
            // block size | inode size | block number |      result |
            "           8 |         32 |            0 |             ", // 16 bytes for iblock, 1 direct + 3 indirect, 2 ids per block, level counts 1, 2, 4, 8 -> max count 15
            "           8 |         32 |            1 |           0 ",
            "           8 |         32 |            2 |           1 ",
            "           8 |         32 |            3 |         0,0 ",
            "           8 |         32 |            4 |         0,1 ",
            "           8 |         32 |            5 |         1,0 ",
            "           8 |         32 |            6 |         1,1 ",
            "           8 |         32 |            7 |       0,0,0 ",
            "           8 |         32 |            8 |       0,0,1 ",
            "           8 |         32 |            9 |       0,1,0 ",
            "           8 |         32 |           10 |       0,1,1 ",
            "           8 |         32 |           11 |       1,0,0 ",
            "           8 |         32 |           12 |       1,0,1 ",
            "           8 |         32 |           13 |       1,1,0 ",
            "           8 |         32 |           14 |       1,1,1 ",
            "          32 |         32 |            0 |             ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block, level counts 1, 8, 64, 512 -> max count 585
            "          32 |         32 |            1 |           0 ",
            "          32 |         32 |            8 |           7 ",
            "          32 |         32 |            9 |         0,0 ",
            "          32 |         32 |           72 |         7,7 ",
            "          32 |         32 |           73 |       0,0,0 ",
            "          32 |         32 |          584 |       7,7,7 ",
            "         512 |         32 |            0 |             ", // 16 bytes for iblock, 1 direct + 3 indirect, 128 ids per block, level counts 1, 128, 16_384, 2_097_152 -> max count 2_113_665
            "         512 |         32 |            1 |           0 ",
            "         512 |         32 |          128 |         127 ",
            "         512 |         32 |          129 |         0,0 ",
            "         512 |         32 |        16512 |     127,127 ",
            "         512 |         32 |        16513 |       0,0,0 ",
            "         512 |         32 |        16514 |       0,0,1 ",
            "         512 |         32 |      2113664 | 127,127,127 ",
    }, delimiter = '|')
    @ParameterizedTest
    void addressOffsets_should_calculateListOfNumbers(int blockSize, int inodeSize, int blockNumber, @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedCoordinates) {
        // When
        var result = BlockMapping.addressOffsets(blockSize, inodeSize, blockNumber);
        // Then
        assertThat(result).containsExactlyElementsOf(expectedCoordinates);
    }
    //endregion

    //region BlockMapping::numberOfAddresses
    @CsvSource(value = {
            // Call                                  | Expected |
            // block size | inode size | block count |   result |
            "          32 |         32 |           0 |        0 ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block
            "          32 |         32 |           1 |        1 ", // 1
            "          32 |         32 |           2 |        2 ",
            "          32 |         32 |           3 |        2 ",
            "          32 |         32 |           9 |        2 ", // 1 + 8
            "          32 |         32 |          10 |        3 ",
            "          32 |         32 |          73 |        3 ", // 1 + 8 + 8^2
            "          32 |         32 |          74 |        4 ",
            "          32 |         32 |         585 |        4 ", // 1 + 8 + 8^2 + 8^3
            "          32 |         32 |         586 |        5 ",

            "          64 |         64 |           0 |        0 ", // 48 bytes for iblock, 9 direct + 3 indirect, 16 ids per block
            "          64 |         64 |           1 |        1 ",
            "          64 |         64 |           9 |        9 ", // 9
            "          64 |         64 |          10 |       10 ",
            "          64 |         64 |          25 |       10 ", // 9 + 16
            "          64 |         64 |          26 |       11 ",
            "          64 |         64 |         281 |       11 ", // 9 + 16 + 16^2
            "          64 |         64 |         282 |       12 ",
            "          64 |         64 |        4377 |       12 ", // 9 + 16 + 16^2 + 16^3
            "          64 |         64 |        4378 |       13 ",

            "         128 |         64 |           0 |        0 ", // 48 bytes for iblock, 9 direct + 3 indirect, 32 ids per block
            "         128 |         64 |           1 |        1 ",
            "         128 |         64 |           9 |        9 ", // 9
            "         128 |         64 |          10 |       10 ",
            "         128 |         64 |          41 |       10 ", // 9 + 32
            "         128 |         64 |          42 |       11 ",
            "         128 |         64 |        1065 |       11 ", // 9 + 32 + 32^2
            "         128 |         64 |        1066 |       12 ",
            "         128 |         64 |       33833 |       12 ", // 9 + 32 + 32^2 + 32^3
            "         128 |         64 |       33834 |       13 ",

            "        4096 |        128 |           0 |        0 ", // 48 bytes for iblock, 25 direct + 3 indirect, 1024 ids per block
            "        4096 |        128 |           1 |        1 ",
            "        4096 |        128 |          25 |       25 ", // 25
            "        4096 |        128 |          26 |       26 ",
            "        4096 |        128 |          27 |       26 ",
            "        4096 |        128 |        1049 |       26 ", // 25 + 1024
            "        4096 |        128 |        1050 |       27 ",
            "        4096 |        128 |     1049625 |       27 ", // 25 + 1024 + 1024^2
            "        4096 |        128 |     1049626 |       28 ",
            "        4096 |        128 |  1074791449 |       28 ", // 25 + 1024 + 1024^2 + 1024^3
            "        4096 |        128 |  1074791450 |       29 ",
    }, delimiter = '|')
    @ParameterizedTest
    void numberOfAddresses_should_calculateNumber(int blockSize, int inodeSize, int blocksCount, int expectedResult) {
        // When
        var result = BlockMapping.numberOfAddresses(blockSize, inodeSize, blocksCount);
        // Then
        assertThat(result).isEqualTo(expectedResult);
    }
    //endregion

    //region BlockMapping::canGrow
    @CsvSource(value = {
            // Call                                  | Expected |
            // block size | inode size | block count |   result |
            "          32 |         32 |           0 |     true ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block
            "          32 |         32 |           1 |     true ", // 1
            "          32 |         32 |           2 |     true ",
            "          32 |         32 |           3 |     true ",
            "          32 |         32 |           9 |     true ", // 1 + 8
            "          32 |         32 |          10 |     true ",
            "          32 |         32 |          73 |     true ", // 1 + 8 + 8^2
            "          32 |         32 |          74 |     true ",
            "          32 |         32 |         585 |    false ", // 1 + 8 + 8^2 + 8^3
            "          32 |         32 |         586 |    false ",

            "          64 |         64 |           0 |     true ", // 48 bytes for iblock, 9 direct + 3 indirect, 16 ids per block
            "          64 |         64 |           1 |     true ",
            "          64 |         64 |           9 |     true ", // 9
            "          64 |         64 |          10 |     true ",
            "          64 |         64 |          25 |     true ", // 9 + 16
            "          64 |         64 |          26 |     true ",
            "          64 |         64 |         281 |     true ", // 9 + 16 + 16^2
            "          64 |         64 |         282 |     true ",
            "          64 |         64 |        4377 |    false ", // 9 + 16 + 16^2 + 16^3
            "          64 |         64 |        4378 |    false ",

            "         128 |         64 |           0 |     true ", // 48 bytes for iblock, 9 direct + 3 indirect, 32 ids per block
            "         128 |         64 |           1 |     true ",
            "         128 |         64 |           9 |     true ", // 9
            "         128 |         64 |          10 |     true ",
            "         128 |         64 |          41 |     true ", // 9 + 32
            "         128 |         64 |          42 |     true ",
            "         128 |         64 |        1065 |     true ", // 9 + 32 + 32^2
            "         128 |         64 |        1066 |     true ",
            "         128 |         64 |       33833 |    false ", // 9 + 32 + 32^2 + 32^3
            "         128 |         64 |       33834 |    false ",

            "        4096 |        128 |           0 |     true ", // 48 bytes for iblock, 25 direct + 3 indirect, 1024 ids per block
            "        4096 |        128 |           1 |     true ",
            "        4096 |        128 |          25 |     true ", // 25
            "        4096 |        128 |          26 |     true ",
            "        4096 |        128 |          27 |     true ",
            "        4096 |        128 |        1049 |     true ", // 25 + 1024
            "        4096 |        128 |        1050 |     true ",
            "        4096 |        128 |     1049625 |     true ", // 25 + 1024 + 1024^2
            "        4096 |        128 |     1049626 |     true ",
            "        4096 |        128 |  1074791449 |    false ", // 25 + 1024 + 1024^2 + 1024^3
            "        4096 |        128 |  1074791450 |    false ",
    }, delimiter = '|')
    @ParameterizedTest
    void canGrow_should_calculateTrueOrFalse(int blockSize, int inodeSize, int blocksCount, boolean expectedResult) {
        // When
        var result = BlockMapping.canGrow(blockSize, inodeSize, blocksCount);
        // Then
        assertThat(result).isEqualTo(expectedResult);
    }
    //endregion

    //region BlockMapping::resolve
    @CsvSource(value = {
            // Given                                                                  | Call         | Expected                                          |
            // block size | inode size | blocks count |                     addresses | block number | read block             | read offset |     result |
            "          32 |         32 |          500 |                       5,6,7,8 |            0 |                        |             |          5 ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block
            "          32 |         32 |          500 |                       5,6,7,8 |            1 |                      6 |           0 |         36 ",
            "          32 |         32 |          500 |                       5,6,7,8 |            2 |                      6 |           1 |         37 ",
            "          32 |         32 |          500 |                       5,6,7,8 |            8 |                      6 |           7 |         43 ", // index 8  -> count 9  -> levels counts 1, 8     -> 1 level  -> block 6 offset 7 -> 43 (6*6+7)
            "          32 |         32 |          500 |                       5,6,7,8 |            9 |                   7,49 |         0,0 |       2401 ", // index 9  -> count 10 -> levels counts 1, 8, 1  -> 2 levels -> block 7 offset 0 -> 49 (7*7+0) -> block 49 offset 0 -> 2401 (49*49+0)
            "          32 |         32 |          500 |                       5,6,7,8 |           72 |                   7,56 |         7,7 |       3143 ", // index 72 -> count 73 -> levels counts 1, 8, 64 -> 2 levels -> block 7 offset 7 -> 56 (7*7+7) -> block 56 offset 7 -> 3143 (56*56+0)
            "          32 |         32 |          500 |                       5,6,7,8 |           73 |              8,64,4096 |       0,0,0 |   16777216 ",
            "         128 |         64 |        35000 | 1,2,3,4,5,6,7,8,9,10,11,12,13 |        33832 |           12,175,30656 |    31,31,31 |  939790367 ",
            "         128 |         64 |        35000 | 1,2,3,4,5,6,7,8,9,10,11,12,13 |        33833 | 13,169,28561,815730721 |     0,0,0,0 | 1778525249 ",
    }, delimiter = '|')
    @ParameterizedTest
    void resolve_should_calculateTrueOrFalse(
            int blockSize,
            int inodeSize,
            int blocksCount,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> addresses,
            int blockNumber,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> expectedIndirectBlockIds,
            @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedIndirectBlockOffsets,
            @ConvertWith(BlockIdConverter.class) Block.Id expectedResult,
            @Mock Inode inode,
            @Captor ArgumentCaptor<Block.Id> indirectBlockIdCaptor,
            @Captor ArgumentCaptor<Integer> indirectBlockOffsetCaptor) {
        // Given
        var blockMapping = spy(new BlockMapping(inode, addresses));
        when(inode.blockSize()).thenReturn(blockSize);
        when(inode.inodeSize()).thenReturn(inodeSize);
        when(inode.blocksCount()).thenReturn(blocksCount);
        // When read block N then read value of N * N + offset
        lenient().doAnswer(invocation -> {
            var blockId = invocation.getArgument(0, Block.Id.class);
            var offset = invocation.getArgument(1, Integer.class);
            return Block.Id.of(blockId.value() * blockId.value() + offset);
        }).when(blockMapping).readBlockId(indirectBlockIdCaptor.capture(), indirectBlockOffsetCaptor.capture());
        // When
        var result = blockMapping.resolve(blockNumber);
        // Then
        assertSoftly(softly -> {
            softly.assertThat(indirectBlockIdCaptor.getAllValues()).containsExactlyElementsOf(expectedIndirectBlockIds);
            softly.assertThat(indirectBlockOffsetCaptor.getAllValues()).containsExactlyElementsOf(expectedIndirectBlockOffsets);
            softly.assertThat(result).isEqualTo(expectedResult);
        });
    }

    @Test
    void resolve_shouldThrowIndexOutOfBoundsException_when_blockNumberIsMoreThatBlocksCount(@Mock Inode inode) {
        // Given
        var blockMapping = new BlockMapping(inode, List.of());
        when(inode.blocksCount()).thenReturn(1);
        // When Then
        assertThatThrownBy(() -> blockMapping.resolve(1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
    //endregion


    @CsvSource(value = {
            // Given                                              | Expected                                                                                        |
            // block size | inode size | blocks count | addresses | read block | read offset | write block    | write offset |    write value |  addresses | result |
            "          32 |         32 |            1 |         5 |            |             |           1500 |            0 |           1501 |     5,1500 |   1501 ", // 16 bytes for iblock, 1 direct + 3 indirect, 8 ids per block
            "          32 |         32 |            2 |       5,6 |            |             |              6 |            1 |           1500 |        5,6 |   1500 ",
            "          32 |         32 |            3 |       5,6 |            |             |              6 |            2 |           1500 |        5,6 |   1500 ",
            "          32 |         32 |            8 |       5,6 |            |             |              6 |            7 |           1500 |        5,6 |   1500 ",
            "          32 |         32 |            9 |       5,6 |            |             |      1501,1500 |          0,0 |      1502,1501 |   5,6,1500 |   1502 ",
            "          32 |         32 |           10 |     5,6,7 |          7 |           0 |             49 |            1 |           1500 |      5,6,7 |   1500 ",
            "          32 |         32 |           11 |     5,6,7 |          7 |           0 |             49 |            2 |           1500 |      5,6,7 |   1500 ",
            "          32 |         32 |           72 |     5,6,7 |          7 |           7 |             56 |            7 |           1500 |      5,6,7 |   1500 ",
            "          32 |         32 |           73 |     5,6,7 |            |             | 1502,1501,1500 |        0,0,0 | 1503,1502,1501 | 5,6,7,1500 |   1503 ",
            "          32 |         32 |           74 |   5,6,7,8 |       8,64 |         0,0 |           4096 |            1 |           1500 |    5,6,7,8 |   1500 ",
            "          32 |         32 |          584 |   5,6,7,8 |       8,71 |         7,7 |           5048 |            7 |           1500 |    5,6,7,8 |   1500 ",
            "          64 |         40 |            1 |         5 |            |             |                |              |                |     5,1500 |   1500 ", // 24 bytes for iblock, 3 direct + 3 indirect, 16 ids per block
            "          64 |         40 |            2 |       5,6 |            |             |                |              |                |   5,6,1500 |   1500 ",
            "          64 |         40 |            3 |     5,6,7 |            |             |           1500 |            0 |           1501 | 5,6,7,1500 |   1501 ",
    }, delimiter = '|')
    @ParameterizedTest
    void allocate_should_calculateTrueOrFalse(
            int blockSize,
            int inodeSize,
            int blocksCount,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> addresses,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> expectedReadBlockIds,
            @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedReadBlockOffsets,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> expectedWriteBlockIds,
            @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedWriteBlockOffsets,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> expectedWriteBlockValues,
            @ConvertWith(BlockIdListConverter.class) List<Block.Id> expectedAddresses,
            @ConvertWith(BlockIdConverter.class) Block.Id expectedResult,
            @Mock Inode inode,
            @Captor ArgumentCaptor<Block.Id> readBlockIdCaptor,
            @Captor ArgumentCaptor<Integer> readBlockOffsetCaptor,
            @Captor ArgumentCaptor<Block.Id> writeBlockIdCaptor,
            @Captor ArgumentCaptor<Integer> writeBlockOffsetCaptor,
            @Captor ArgumentCaptor<Block.Id> writeBlockValueCaptor) throws BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException {
        // Given
        var blockMapping = spy(new BlockMapping(inode, new ArrayList<>(addresses)));
        when(inode.blockSize()).thenReturn(blockSize);
        when(inode.inodeSize()).thenReturn(inodeSize);
        when(inode.blocksCount()).thenReturn(blocksCount);
        when(inode.reserveBlocks(anyInt())).thenAnswer(invocation -> List.of(Block.Range.of(Block.Id.of(1500), invocation.getArgument(0, Integer.class))));
        // When read block N then read value of N * N + offset
        lenient().doAnswer(invocation -> {
            var blockId = invocation.getArgument(0, Block.Id.class);
            var offset = invocation.getArgument(1, Integer.class);
            return Block.Id.of(blockId.value() * blockId.value() + offset);
        }).when(blockMapping).readBlockId(readBlockIdCaptor.capture(), readBlockOffsetCaptor.capture());
        lenient().doNothing().when(blockMapping).writeBlockId(writeBlockIdCaptor.capture(), writeBlockOffsetCaptor.capture(), writeBlockValueCaptor.capture());
        // When
        var result = blockMapping.allocate();
        // Then
        assertSoftly(softly -> {
            softly.assertThat(blockMapping.getAddresses()).containsExactlyElementsOf(expectedAddresses);
            softly.assertThat(readBlockIdCaptor.getAllValues()).containsExactlyElementsOf(expectedReadBlockIds);
            softly.assertThat(readBlockOffsetCaptor.getAllValues()).containsExactlyElementsOf(expectedReadBlockOffsets);
            softly.assertThat(writeBlockIdCaptor.getAllValues()).containsExactlyElementsOf(expectedWriteBlockIds);
            softly.assertThat(writeBlockOffsetCaptor.getAllValues()).containsExactlyElementsOf(expectedWriteBlockOffsets);
            softly.assertThat(writeBlockValueCaptor.getAllValues()).containsExactlyElementsOf(expectedWriteBlockValues);
            softly.assertThat(result).isEqualTo(expectedResult);
        });
    }

    private static class BlockIdListConverter implements ArgumentConverter {

        private final BlockIdConverter blockIdConverter = new BlockIdConverter();

        @Override
        public final List<Block.Id> convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(source.toString().split(","))
                    .map(String::trim)
                    .map(str -> blockIdConverter.convert(str, context))
                    .toList();
        }

    }

    private static class BlockIdConverter implements ArgumentConverter {

        @Override
        public final Block.Id convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return null;
            }
            return Block.Id.of(Integer.parseInt(source.toString()));
        }

    }

}

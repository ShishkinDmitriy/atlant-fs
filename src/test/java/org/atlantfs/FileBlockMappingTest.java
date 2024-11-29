package org.atlantfs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class FileBlockMappingTest {

    @InjectMocks
    private @Spy FileBlockMapping fileBlockMapping;
    private @Mock Inode inode;
    private @Mock AtlantFileSystem fileSystem;
    private final List<DataBlock> dataBlocks = new ArrayList<>();

    @BeforeEach
    void beforeEach() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        lenient().when(fileSystem.blockSize()).thenReturn(8);
        lenient().when(fileSystem.inodeSize()).thenReturn(32);
        lenient().when(inode.blockSize()).thenReturn(8);
        lenient().when(inode.getFileSystem()).thenReturn(fileSystem);
        lenient().doAnswer(invocation -> dataBlocks.get(invocation.getArgument(0, Integer.class))).when(fileBlockMapping).get(anyInt());
        lenient().doAnswer(invocation -> {
            dataBlocks.add(invocation.getArgument(0, DataBlock.class));
            fileBlockMapping.blocksCount++;
            return null;
        }).when(fileBlockMapping).add(any(DataBlock.class));
    }

    //region FileBlockMapping::read
    @CsvSource(value = {
            // Given                         | Call       buffer               | Expected                 |
            // block 0 | block 1  | block 2  | position | position | remaining | result                   |
            "          |          |          |        0 |        0 |         1 |                          ",
            "          |          |          |        1 |        0 |         1 |                          ",
            "          |          |          |        1 |        1 |         1 |                          ",
            " 0        |          |          |        0 |        0 |         1 |                        0 ",
            " 0        |          |          |        1 |        0 |         1 |                          ",
            " 0        |          |          |        1 |        1 |         1 |                          ",
            " 01234567 | 89       |          |        0 |        0 |        10 |               0123456789 ",
            " 01234567 | 89       |          |        0 |        7 |         3 |                      789 ",
            " 01234567 | 89       |          |       10 |        0 |        16 |                          ",
            " 01234567 | 89       |          |        0 |       10 |        16 |                          ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |         1 |                        0 ",
            " 01234567 | 89abcdef | ghijklmn |        1 |        0 |         1 |                        1 ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        1 |         1 |                        1 ",
            " 01234567 | 89abcdef | ghijklmn |        7 |        0 |         1 |                        7 ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        7 |         1 |                        7 ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |         8 |                 01234567 ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        8 |         8 |                 89abcdef ",
            " 01234567 | 89abcdef | ghijklmn |        0 |       16 |         8 |                 ghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |       15 |        0 |         2 |                       fg ",
            " 01234567 | 89abcdef | ghijklmn |        0 |       15 |         2 |                       fg ",
            " 01234567 | 89abcdef | ghijklmn |        5 |        1 |        15 |          6789abcdefghijk ",
            " 01234567 | 89abcdef | ghijklmn |        5 |        1 |        18 |       6789abcdefghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |        5 |        1 |        19 |       6789abcdefghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |        24 | 0123456789abcdefghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |        25 | 0123456789abcdefghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |      1024 | 0123456789abcdefghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |       23 |        0 |      1024 |                        n ",
            " 01234567 | 89abcdef | ghijklmn |        0 |       23 |      1024 |                        n ",
            " 01234567 | 89abcdef | ghijklmn |       24 |        0 |         1 |                          ",
            " 01234567 | 89abcdef | ghijklmn |        0 |       24 |         1 |                          ",
    }, delimiter = '|')
    @ParameterizedTest
    void read_should_putBytesToBuffer(
            String block0,
            String block1,
            String block2,
            long position,
            int bufferPosition,
            int bufferRemaining,
            String expectedRead) throws DataOutOfMemoryException, BitmapRegionOutOfMemoryException {
        // Given
        addDataBlock(block0);
        addDataBlock(block1);
        addDataBlock(block2);
        var buffer = ByteBuffer.allocate(bufferPosition + bufferRemaining);
        buffer.position(bufferPosition);
        // When
        var result = fileBlockMapping.read(position, buffer);
        // Then
        if (expectedRead != null) {
            assertThat(result).isEqualTo(expectedRead.length());
            buffer.rewind();
            var bytes = new byte[expectedRead.length()];
            buffer.get(bufferPosition, bytes);
            assertThat(new String(bytes)).isEqualTo(expectedRead);
        } else {
            assertThat(result).isEqualTo(0);
        }
    }
    //endregion

    //region FileBlockMapping::write
    @CsvSource(value = {
            // Given                         | Call       buffer                               | Given                          |
            // block 0 | block 1  | block 2  | position | position | bytes                     | block 0  | block 1  | block 2  |
            "       '' |          |          |        0 |        0 |                         0 | 0        |          |          ",
            " 1        |          |          |        0 |        0 |                         0 | 0        |          |          ",
            "       '' |          |          |        7 |        0 |                         0 | .......0 |          |          ",
            "       '' |          |          |        0 |        7 |                         0 | .......0 |          |          ",
            "       '' |          |          |        0 |        0 |                  01234567 | 01234567 |          |          ",
            "       '' |          |          |        1 |        0 |                  01234567 | .0123456 | 7        |          ",
            "       '' |          |          |        4 |        0 |                  01234567 | ....0123 | 4567     |          ",
            "       '' |          |          |        8 |        0 |                  01234567 | ........ | 01234567 |          ",
            "       '' |          |          |       12 |        0 |                  01234567 | ........ | ....0123 | 4567     ",
            "       '' |          |          |       16 |        0 |                  01234567 | ........ | ........ | 01234567 ",
            "       '' |          |          |        0 |        0 |  0123456789abcdefghijklmn | 01234567 | 89abcdef | ghijklmn ",
            " ........ |       '' |          |        0 |        0 |                  01234567 | 01234567 |          |          ",
            " ........ |       '' |          |        1 |        0 |                  01234567 | .0123456 | 7        |          ",
            " ........ |       '' |          |        0 |        1 |                  01234567 | .0123456 | 7        |          ",
            " ........ |       '' |          |        4 |        0 |                  01234567 | ....0123 | 4567     |          ",
            " ........ |       '' |          |        0 |        4 |                  01234567 | ....0123 | 4567     |          ",
            " ........ |       '' |          |        8 |        0 |                  01234567 | ........ | 01234567 |          ",
            " ........ |       '' |          |        0 |        8 |                  01234567 | ........ | 01234567 |          ",
            " ........ |       '' |          |       12 |        0 |                  01234567 | ........ | ....0123 | 4567     ",
            " ........ |       '' |          |        0 |       12 |                  01234567 | ........ | ....0123 | 4567     ",
            " 01234567 | 89ab.... |          |        0 |       12 |                  ghijklmn | 01234567 | 89abghij | klmn     ",
            " ........ |       '' |          |       16 |        0 |                  01234567 | ........ | ........ | 01234567 ",
            " ........ |       '' |          |        0 |       16 |                  01234567 | ........ | ........ | 01234567 ",
            " 89abcdef | ghijklmn |          |       16 |        0 |                  01234567 | 89abcdef | ghijklmn | 01234567 ",
            " 89abcdef | ghijklmn |          |        0 |       16 |                  01234567 | 89abcdef | ghijklmn | 01234567 ",
            " ........ |       '' |          |        0 |        0 |  0123456789abcdefghijklmn | 01234567 | 89abcdef | ghijklmn ",
            " ........ | ........ |       '' |        0 |        0 |  0123456789abcdefghijklmn | 01234567 | 89abcdef | ghijklmn ",
            " 01234567 | 89abcdef | ghijklmn |        0 |        0 |  89abcdefghijklmn01234567 | 89abcdef | ghijklmn | 01234567 ",
    }, delimiter = '|')
    @ParameterizedTest
    void write_should_getBytesFromBuffer(
            String block0,
            String block1,
            String block2,
            long position,
            int bufferPosition,
            String bufferValue,
            String expectedBlock0,
            String expectedBlock1,
            String expectedBlock2) throws DataOutOfMemoryException, BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        // Given
        addDataBlock(block0);
        addDataBlock(block1);
        addDataBlock(block2);
        var buffer = ByteBuffer.allocate(bufferPosition + bufferValue.length());
        buffer.position(bufferPosition);
        buffer.put(bufferValue.getBytes(StandardCharsets.UTF_8));
        buffer.flip();
        buffer.position(bufferPosition);
        // When
        var result = fileBlockMapping.write(position, buffer);
        // Then
        assertThat(result).isEqualTo(bufferValue.length());
        compare(0, expectedBlock0);
        compare(1, expectedBlock1);
        compare(2, expectedBlock2);
    }
    //endregion

    private void compare(int index, String expectedBlock) {
        if (expectedBlock != null) {
            assertThat(dataBlocks.get(index).bytes()).containsExactly(bytes(expectedBlock));
        }
    }

    private void addDataBlock(String value) throws BitmapRegionOutOfMemoryException {
        if (value != null) {
            var dataBlock = DataBlock.init(fileSystem, bytes(value));
            dataBlocks.add(dataBlock);
            fileBlockMapping.size += value.length();
            fileBlockMapping.blocksCount++;
        }
    }

    private static byte[] bytes(String expectedBlock) {
        return expectedBlock.replace('.', (char) 0)
                .getBytes(StandardCharsets.UTF_8);
    }

}

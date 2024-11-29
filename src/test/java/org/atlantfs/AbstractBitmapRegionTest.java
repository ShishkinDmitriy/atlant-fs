package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.atlantfs.util.ByteBufferUtil.blocks;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, LoggingExtension.class})
class AbstractBitmapRegionTest {

    public static final int BLOCK_SIZE = 2;
    public static final Block.Id BITMAP_REGION_FIRST_BLOCK = Block.Id.of(9);
    public static final int BITMAP_REGION_NUMBER_OF_BLOCKS = 4;
    @Mock
    AtlantFileSystem fileSystem;
    @Mock
    SuperBlock superBlock;

    record Id(int value) implements org.atlantfs.Id {
    }

    record Range(Id from, int length) implements org.atlantfs.Range<Id> {
    }

    BitmapRegion<Id, Range> bitmapRegion;

    @BeforeEach
    void beforeEach() {
        lenient().when(fileSystem.superBlock()).thenReturn(superBlock);
        lenient().when(fileSystem.getBlockByteBuffer()).thenCallRealMethod();
        lenient().when(fileSystem.blockSize()).thenReturn(BLOCK_SIZE);
        lenient().when(superBlock.blockSize()).thenReturn(BLOCK_SIZE);
        lenient().when(superBlock.firstBlockOfInodeBitmap()).thenReturn(BITMAP_REGION_FIRST_BLOCK);
        lenient().when(superBlock.numberOfInodeBitmaps()).thenReturn(BITMAP_REGION_NUMBER_OF_BLOCKS);
        bitmapRegion = new BitmapRegion<>(fileSystem) {

            @Override
            public Block.Id firstBlock() {
                return BITMAP_REGION_FIRST_BLOCK;
            }

            @Override
            public int numberOfBlocks() {
                return BITMAP_REGION_NUMBER_OF_BLOCKS;
            }

            @Override
            public int blockSize() {
                return BLOCK_SIZE;
            }

            @Override
            int toBitmapNumber(Id id) {
                return id.value() / (blockSize() * 8);
            }

            @Override
            int toBitmapOffset(Id id) {
                return id.value() % (blockSize() * 8);
            }

            @Override
            Range applyOffset(int bitmapNumber, Bitmap.Range range) {
                return new Range(applyOffset(bitmapNumber, range.from()), range.length());
            }

            @Override
            Id applyOffset(int bitmapNumber, int position) {
                return new Id(bitmapNumber * blockSize() * 8 + position);
            }
        };
    }

    //region InodeBitmap::reserve
    @CsvSource(value = {
            // Given              | Expected    |
            // blocks             | res  | curr |
            " 0000 0000 0000 0000 |    0 |    0 ",
            " ffff ffff ffff feff |   48 |    3 ",
            " ffff ffff ffff efff |   52 |    3 ",
            " ffff ffff ffff fffe |   56 |    3 ",
            " ffff ffff ffff ffef |   60 |    3 ",
    }, delimiter = '|')
    @ParameterizedTest(name = "Given blocks [{0}] when reserve single item then should return id [{1}], and current bitmap should be changed to [{2}]")
    void reserve_should_findFirstFreeBitInRequiredBlock(String blocksHex, int expected, int expectedCurrent) throws BitmapRegionNotEnoughSpaceException {
        // Given
        var blocks = blocks(blocksHex);
        configureFileSystem(blocks);
        // When
        var result = bitmapRegion.reserve();
        // Then
        assertThat(result).isEqualTo(new Id(expected));
        assertThat(bitmapRegion.getCurrent()).isEqualTo(expectedCurrent);
    }

    @Test
    void reserve_should_throwBitmapRegionOutOfMemoryException_when_allBitsAreSet() {
        // Given
        var blocks = blocks("ffff ffff ffff ffff");
        configureFileSystem(blocks);
        // When Then
        assertThatThrownBy(bitmapRegion::reserve)
                .isInstanceOf(BitmapRegionNotEnoughSpaceException.class);
    }

    @Test
    void reserve_should_throwBitmapRegionOutOfMemoryException_when_allBitsAreSetAndRequireMultiple() {
        // Given
        var blocks = blocks("ffff ffff ffff ffff");
        configureFileSystem(blocks);
        // When Then
        assertThatThrownBy(() -> bitmapRegion.reserve(2))
                .isInstanceOf(BitmapRegionNotEnoughSpaceException.class);
    }
    //endregion

    //region InodeBitmap::free
    @CsvSource(value = {
            // Given              | Call | Expected         |
            // blocks             | del  | write  | written |
            " 0000 0000 0000 0000 |    0 |  false |      -1 ",
            " ffff ffff ffff ffff |    8 |   true |       0 ",
            " ffff ffff ffff ffff |   12 |   true |       0 ",
            " ffff ffff ffff ffff |   63 |   true |       3 ",
    }, delimiter = '|')
    @ParameterizedTest
    void free_should_setSpecifiedBitToZeroInRequiredBlock(String blocksHex, int inodeId, boolean expectedWrite, int expectedWritten) {
        // Given
        var blocks = blocks(blocksHex);
        configureFileSystem(blocks);
        // When
        bitmapRegion.free(new Id(inodeId));
        // Then
        assertThat(bitmapRegion.getCurrent()).isEqualTo(0); // Initialized with 0
        if (expectedWrite) {
            verify(fileSystem).writeBlock(eq(BITMAP_REGION_FIRST_BLOCK.plus(expectedWritten)), any());
        } else {
            verify(fileSystem, times(0)).writeBlock(any(), any());
        }
    }
    //endregion

    @Test
    void reserveAllThenFree() throws BitmapRegionNotEnoughSpaceException {
        // Given
        var blocks = blocks("0000 0000 0000 0000");
        configureFileSystem(blocks);
        var totalSize = BITMAP_REGION_NUMBER_OF_BLOCKS * BLOCK_SIZE * 8;
        // When
        var reserved = bitmapRegion.reserve(totalSize);
        // Then
        assertThat(toHex(blocks)).isEqualTo("ffff ffff ffff ffff");
        assertThat(reserved).containsExactlyElementsOf(
                IntStream.range(0, BITMAP_REGION_NUMBER_OF_BLOCKS).mapToObj(i -> new Range(new Id(i * BLOCK_SIZE * 8), BLOCK_SIZE * 8)).toList()
        );
        // When
        var ranges = IntStream.range(0, totalSize)
                .filter(i -> i % 8 == 0)
                .mapToObj(i -> new Range(new Id(i), 2))
                .toList();
        bitmapRegion.freeRanges(ranges);
        // Then
        assertThat(toHex(blocks)).isEqualTo("fcfc fcfc fcfc fcfc");
        // When
        var reserved2 = bitmapRegion.reserve(16);
        // Then
        assertThat(toHex(blocks)).isEqualTo("ffff ffff ffff ffff");
        assertThat(reserved2).containsExactlyElementsOf(
                IntStream.range(0, 8).mapToObj(i -> new Range(new Id(i * 8), 2)).toList()
        );
    }

    private void configureFileSystem(List<ByteBuffer> blocks) {
        when(fileSystem.readBlock(any(Block.Id.class))).thenAnswer(invocation -> {
            var arg = invocation.getArgument(0, Block.Id.class);
            var value = arg.minus(BITMAP_REGION_FIRST_BLOCK).value();
            return blocks.get(value);
        });
        lenient().doAnswer(invocation -> {
            var arg0 = invocation.getArgument(0, Block.Id.class);
            @SuppressWarnings("unchecked") Consumer<ByteBuffer> arg1 = invocation.getArgument(1, Consumer.class);
            var value = arg0.minus(BITMAP_REGION_FIRST_BLOCK).value();
            var buffer = blocks.get(value);
            buffer.clear();
            arg1.accept(buffer);
            return null;
        }).when(fileSystem).writeBlock(any(Block.Id.class), any());
    }

    private String toHex(List<ByteBuffer> buffers) {
        return buffers.stream()
                .map(buffer -> HexFormat.of().formatHex(buffer.array()))
                .collect(Collectors.joining(" "));
    }

}

package org.atlantfs;

import org.atlantfs.util.CommaSeparatedListConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IndirectBlockTest {

    private final AtomicInteger rangeReserve = new AtomicInteger(100);
    private final AtomicInteger singleReserve = new AtomicInteger(1000);
    private @Mock AtlantFileSystem fileSystem;
    private @Mock Block leaf;
    private @Mock Function<Block.Id, Block> reader;

    @BeforeEach
    void beforeEach() throws BitmapRegionOutOfMemoryException {
        lenient().when(fileSystem.reserveBlocks(anyInt())).thenAnswer(invocation -> {
            var size = invocation.getArgument(0, Integer.class);
            return List.of(Block.Range.of(Block.Id.of(rangeReserve.getAndAdd(size)), size));
        });
        lenient().when(fileSystem.reserveBlock()).thenAnswer(_ -> Block.Id.of(singleReserve.getAndIncrement()));
        lenient().when(reader.apply(any(Block.Id.class))).thenAnswer(invocation -> {
            var id = invocation.getArgument(0, Block.Id.class);
            Block block = mock(Block.class);
            lenient().when(block.id()).thenReturn(id);
            return block;
        });
        lenient().when(leaf.id()).thenReturn(Block.Id.of(7));
    }

    //region IndirectBlock::init
    @CsvSource(value = {
            // Call  | Expected                                         |
            // depth | blockIds            | dirty            | depth   |
            "      0 |                1000 |                7 |       0 ",
            "      1 |           1001,1000 |           1000;7 |     1,0 ",
            "      2 |      1002,1001,1000 |      1001;1000;7 |   2,1,0 ",
            "      3 | 1003,1002,1001,1000 | 1002;1001;1000;7 | 3,2,1,0 ",
    }, delimiter = '|')
    @ParameterizedTest
    void init_should_createChainOfIndirectBlocks(
            int depth,
            @ConvertWith(BlockTest.BlockIdListConverter.class) List<Block.Id> expectedBlockIds,
            @ConvertWith(BlockTest.BlockIdLis0fListConverter.class) List<List<Block.Id>> expectedDirtyBlockIds,
            @ConvertWith(CommaSeparatedListConverter.class) List<Integer> expectedDepths) throws BitmapRegionOutOfMemoryException {
        // When
        var result = IndirectBlock.init(fileSystem, depth, reader, leaf);
        // Then
        assertThat(result).isNotNull();
        assertThat(result.id()).isEqualTo(expectedBlockIds.getFirst());
        var chain = collectChain(result);
        assertSoftly(softly -> {
            softly.assertThat(chain).extracting(Block::id).containsExactlyElementsOf(expectedBlockIds);
            softly.assertThat(chain).extracting(Block::isDirty).allMatch(_ -> true);
            softly.assertThat(chain).extracting(IndirectBlock::depth).containsExactlyElementsOf(expectedDepths);
            softly.assertThat(chain).extracting(IndirectBlock::pointers).allMatch(list -> list.size() == 1);
            softly.assertThat(chain).extracting(IndirectBlock::size).allMatch(value -> value == 1);
            softly.assertThat(chain).extracting(IndirectBlock::dirtyBlocks).extracting(blocks -> blocks.stream().map(Block::id).toList()).containsExactlyElementsOf(expectedDirtyBlockIds);
            softly.assertThat(chain.getLast()).extracting(block -> block.pointers().getFirst().computeIfAbsent(_ -> fail())).isEqualTo(leaf);
        });
    }

    @Test
    void init_should_throwNullPointerException_when_fileSystemIsNull() {
        // When Then
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> IndirectBlock.init(null, 0, reader, leaf))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void init_should_throwIllegalArgumentException_when_depthIsNegative() {
        // When Then
        assertThatThrownBy(() -> IndirectBlock.init(fileSystem, -1, reader, leaf))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void init_should_throwNullPointerException_when_readerIsNull() {
        // When Then
        assertThatThrownBy(() -> IndirectBlock.init(fileSystem, 0, null, leaf))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void init_should_throwNullPointerException_when_leafIsNull() {
        // When Then
        assertThatThrownBy(() -> IndirectBlock.init(fileSystem, 0, reader, null))
                .isInstanceOf(NullPointerException.class);
    }
    //endregion

    //region IndirectBlock::get
    @CsvSource(value = {
            // Given              | Call  | Expected  |
            // block size | depth | index | result    |
            "           8 |     0 |     0 |      1000 ", // 2 ids per block
            "           8 |     0 |     1 |      1001 ",
            "           8 |     1 |     0 |      1000 ",
            "           8 |     1 |     1 |      1001 ",
            "           8 |     1 |     2 |      1002 ",
            "           8 |     1 |     3 |      1003 ",
            "           8 |     2 |     0 |      1000 ",
            "           8 |     2 |     7 |      1007 ",
            "          12 |     0 |     0 |      1000 ", // 3 ids per block
            "          12 |     0 |     2 |      1002 ",
            "          12 |     1 |     0 |      1000 ",
            "          12 |     1 |     8 |      1008 ",
            "          12 |     2 |     0 |      1000 ",
            "          12 |     2 |    26 |      1026 ",
            "          64 |     0 |     0 |      1000 ", // 16 ids per block
            "          64 |     0 |    15 |      1015 ",
            "          64 |     1 |     0 |      1000 ",
            "          64 |     1 |   255 |      1255 ",
            "          64 |     2 |     0 |      1000 ",
            "          64 |     2 |  4095 |      5095 ",
    }, delimiter = '|')
    @ParameterizedTest
    void get_should_retrieveLeafBlock(int blockSize, int depth, int index, @ConvertWith(BlockTest.BlockIdConverter.class) Block.Id expectedResult) {
        // Given
        when(fileSystem.blockSize()).thenReturn(blockSize);
        var root = constructTree(blockSize, depth);
        // When
        var result = root.get(index);
        // Then
        assertThat(result).isNotNull();
        assertThat(result.id()).isEqualTo(expectedResult);
    }

    @CsvSource(value = {
            // Given                     |
            // block size | depth | size |
            "           8 |     0 |    1 ", // 2 ids per block,
            "           8 |     0 |    2 ",
            "           8 |     1 |    1 ",
            "           8 |     1 |    4 ",
            "           8 |     2 |    1 ",
            "           8 |     2 |    8 ",
            "          12 |     0 |    1 ", // 3 ids per block
            "          12 |     0 |    3 ",
            "          12 |     1 |    1 ",
            "          12 |     1 |    9 ",
            "          12 |     2 |    1 ",
            "          12 |     2 |   27 ",
            "          64 |     0 |    1 ", // 16 ids per block
            "          64 |     0 |   16 ",
            "          64 |     1 |    1 ",
            "          64 |     1 |  256 ",
            "          64 |     2 |    1 ",
            "          64 |     2 | 4096 ",
    }, delimiter = '|')
    @ParameterizedTest
    void get_should_throwIndexOutOfBoundsException_when_indexIsTooBig(int blockSize, int depth, int size) {
        // Given
        when(fileSystem.blockSize()).thenReturn(blockSize);
        var root = constructTree(blockSize, depth, size);
        // When Then
        assertThatThrownBy(() -> root.get(size))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
    //endregion

    //region IndirectBlock::size
    @CsvSource(value = {
            // Given              |      |
            // block size | depth | size |
            "           8 |     0 |    2 ", // 2 ids per block,
            "           8 |     0 |    1 ",
            "           8 |     0 |    1 ",
            "           8 |     1 |    1 ",
            "           8 |     1 |    4 ",
            "           8 |     2 |    8 ",
            "           8 |     2 |    8 ",
            "          64 |     0 |   16 ", // 16 ids per block
            "          64 |     1 |  256 ",
            "          64 |     2 |    2 ",
            "          64 |     2 |    2 ",
            "          64 |     2 | 4096 ",
            "          64 |     2 | 4096 ",
    }, delimiter = '|')
    @ParameterizedTest
    void size_should_returnNumberOfLeafBlocks(int blockSize, int depth, int size) {
        // Given
        when(fileSystem.blockSize()).thenReturn(blockSize);
        var root = constructTree(blockSize, depth, size);
        // When
        var result = root.size();
        // Then
        assertThat(result).isEqualTo(size);
    }
    //endregion

    //region IndirectBlock::add
    @CsvSource(value = {
            // Given                     |
            // block size | depth | size |
            "           8 |     0 |    1 ", // 2 ids per block,
            "           8 |     1 |    1 ",
            "           8 |     1 |    2 ",
            "           8 |     1 |    3 ",
            "           8 |     2 |    1 ",
            "           8 |     2 |    7 ",
            "          64 |     0 |   15 ", // 16 ids per block
            "          64 |     1 |    1 ",
            "          64 |     1 |  255 ",
            "          64 |     2 |    1 ",
            "          64 |     2 | 4095 ",
    }, delimiter = '|')
    @ParameterizedTest
    void add_should_appendNEwLeafBlock(int blockSize, int depth, int size) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        // Given
        when(fileSystem.blockSize()).thenReturn(blockSize);
        var root = constructTree(blockSize, depth, size);
        assertThat(root.size()).isEqualTo(size);
        // When
        var result = root.add(leaf);
        // Then
        assertThat(result).isEqualTo(size);
        assertThat(root.size()).isEqualTo(size + 1);
        assertThat(root.get(size)).isEqualTo(leaf);
    }

    @CsvSource(value = {
            // Given                     |
            // block size | depth | size |
            "           8 |     0 |    2 ", // 2 ids per block,
            "           8 |     1 |    4 ",
            "           8 |     2 |    8 ",
            "          64 |     0 |   16 ", // 16 ids per block
            "          64 |     1 |  256 ",
            "          64 |     2 | 4096 ",
    }, delimiter = '|')
    @ParameterizedTest
    void add_should_throwIndirectBlockOutOfMemoryException_when_sizeBecomeBiggerThanLimit(int blockSize, int depth, int size) {
        // Given
        when(fileSystem.blockSize()).thenReturn(blockSize);
        var root = constructTree(blockSize, depth, size);
        assertThat(root.size()).isEqualTo(size);
        // When Then
        assertThatThrownBy(() -> root.add(leaf))
                .isInstanceOf(IndirectBlockOutOfMemoryException.class);
    }
    //endregion

    private IndirectBlock<?> constructTree(int blockSize, int depth) {
        return constructTree(blockSize, depth, IndirectBlock.maxSize(blockSize, depth));
    }

    private IndirectBlock<Block> constructTree(int blockSize, int depth, int size) {
        return constructTreeInternal(blockSize, depth, size - 1);
    }

    private IndirectBlock<Block> constructTreeInternal(int blockSize, int depth, int lastIndex) {
        assert lastIndex >= 0;
        var id = reserveForIndirect();
        var block = new IndirectBlock<>(fileSystem, id, depth, reader);
        if (depth > 0) {
            var maxSize = IndirectBlock.maxSize(blockSize, depth - 1);
            var bounds = lastIndex / maxSize;
            for (int i = 0; i < bounds; i++) {
                var subtree = constructTreeInternal(blockSize, depth - 1, maxSize - 1);
                var pointer = Block.Pointer.of(subtree);
                block.addPointer(pointer);
            }
            var subtree = constructTreeInternal(blockSize, depth - 1, lastIndex % maxSize);
            var pointer = Block.Pointer.of(subtree);
            block.addPointer(pointer);
        } else {
            for (int i = 0; i <= lastIndex; i++) {
                var reserved = reserveForLeaf();
                var pointer = Block.Pointer.of(reserved);
                block.addPointer(pointer);
            }
        }
        return block;
    }

    private Block.Id reserveForLeaf() {
        return Block.Id.of(singleReserve.getAndIncrement()); // Reserve single to get 1000+ ids
    }

    private Block.Id reserveForIndirect() {
        return Block.Id.of(rangeReserve.getAndIncrement()); // Reserve range to get 100+ ids.
    }

    private List<IndirectBlock<?>> collectChain(Block block) {
        var result = new ArrayList<IndirectBlock<?>>();
        while (block instanceof IndirectBlock<?> indirectBlock) {
            result.add(indirectBlock);
            block = indirectBlock.pointers().getFirst().computeIfAbsent(_ -> fail());
        }
        return result;
    }

}

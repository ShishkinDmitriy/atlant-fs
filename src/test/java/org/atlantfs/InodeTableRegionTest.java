package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, LoggingExtension.class})
class InodeTableRegionTest {

    public static final int BLOCK_SIZE = 4096;
    public static final int INODE_SIZE = 128;

    @Mock
    AtlantFileSystem fileSystem;
    @Mock
    SuperBlock superBlock;

    @BeforeEach
    void beforeEach() throws BitmapRegionOutOfMemoryException {
        lenient().when(fileSystem.superBlock()).thenReturn(superBlock);
        lenient().when(fileSystem.blockSize()).thenReturn(BLOCK_SIZE);
        lenient().when(fileSystem.inodeSize()).thenReturn(INODE_SIZE);
        lenient().when(fileSystem.iblockSize()).thenReturn(INODE_SIZE - Inode.MIN_LENGTH);
        lenient().when(fileSystem.reserveInode()).thenReturn(Inode.Id.ROOT).thenReturn(Inode.Id.of(45));
        lenient().when(superBlock.blockSize()).thenReturn(BLOCK_SIZE);
        lenient().when(superBlock.inodeSize()).thenReturn(INODE_SIZE);
        lenient().when(superBlock.firstBlockOfInodeTables()).thenReturn(Block.Id.of(9));
        lenient().when(superBlock.numberOfInodeTables()).thenReturn(120);
    }

    //region InodeTable::get
    @Test
    void get_should_findInodeInChannel(@Mock Inode inode) {
        // Given
        var inodeId = Inode.Id.of(53);
        var inodeTable = new InodeTableRegion(fileSystem);
        lenient().when(fileSystem.readInode(inodeId)).thenReturn(inode);
        // When
        var result = inodeTable.get(inodeId);
        // Then
        assertThat(result).isNotNull();
    }

    @Test
    void get_should_useCache_when_searchSameInodeId(@Mock Inode inode) {
        // Given
        var inodeId = Inode.Id.of(53);
        var inodeTable = new InodeTableRegion(fileSystem);
        lenient().when(fileSystem.readInode(inodeId)).thenReturn(inode);
        // When
        inodeTable.get(inodeId);
        // Then
        verify(fileSystem, times(1)).readInode(inodeId);
        for (int i = 0; i < 5; i++) {
            // When
            inodeTable.get(inodeId);
            // Then
            verify(fileSystem, times(1)).readInode(inodeId);
        }
    }
    //endregion

    //region InodeTable::calcBlock
    @CsvSource(value = {
            // inode | block size | inode size | first | expected |
            "      1 |        128 |        128 |     0 |        0 ",
            "      2 |        128 |        128 |     1 |        2 ",
            "      1 |        256 |        128 |     0 |        0 ",
            "      2 |        256 |        128 |     0 |        0 ",
            "      3 |        256 |        128 |     0 |        1 ",
            "      4 |        256 |        128 |     1 |        2 ",
            "     32 |       4096 |        128 |   150 |      150 ",
            "     33 |       4096 |        128 |   150 |      151 ",
    }, delimiter = '|')
    @ParameterizedTest
    void calcBlock_should_calculateBlockId(int inodeId, int blockSize, int inodeSize, int firstBlock, int expectedResult, @Mock Inode root) {
        // Given
        var inodeTable = new InodeTableRegion(fileSystem, root);
        when(fileSystem.inodeSize()).thenReturn(inodeSize);
        // When
        var result = inodeTable.calcBlock(Inode.Id.of(inodeId), blockSize, Block.Id.of(firstBlock));
        // Then
        assertThat(result).isEqualTo(Block.Id.of(expectedResult));
    }
    //endregion

    //region InodeTable::calcPosition
    @CsvSource(value = {
            // inode | block size | inode size | expected |
            "      1 |        128 |        128 |        0 ",
            "      2 |        128 |        128 |        0 ",
            "      1 |        256 |        128 |        0 ",
            "      2 |        256 |        128 |      128 ",
            "      3 |        256 |        128 |        0 ",
            "      4 |        256 |        128 |      128 ",
            "     32 |       4096 |        128 |     3968 ",
            "     33 |       4096 |        128 |        0 ",
    }, delimiter = '|')
    @ParameterizedTest
    void calcPosition_should_calculatePositionInsideBlock(int inodeId, int blockSize, int inodeSize, int expectedResult, @Mock Inode root) {
        // Given
        var inodeTable = new InodeTableRegion(fileSystem, root);
        when(fileSystem.inodeSize()).thenReturn(inodeSize);
        // When
        var result = inodeTable.calcPosition(Inode.Id.of(inodeId), blockSize);
        // Then
        assertThat(result).isEqualTo(expectedResult);
    }
    //endregion

}

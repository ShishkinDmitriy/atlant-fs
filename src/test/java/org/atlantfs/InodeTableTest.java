package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith({MockitoExtension.class, LoggingExtension.class})
class InodeTableTest {

    @Mock
    AtlantFileSystem fileSystem;
    @Mock
    SuperBlock superBlock;

    @BeforeEach
    void beforeEach() {
        lenient().when(fileSystem.getSuperBlock()).thenReturn(superBlock);
        lenient().when(superBlock.getBlockSize()).thenReturn(4096);
    }

    //region InodeTable::get
    @Test
    void get_should_findInodeInChannel(@Mock SeekableByteChannel channel) {
        // Given
        var inodeTable = new InodeTable(fileSystem, Block.Id.of(9), 120);
        // When
        var result = inodeTable.get(Inode.Id.of(53), channel);
        // Then
        assertThat(result).isNotNull();
    }

    @Test
    void get_should_useCache_when_searchSameInodeId(@Mock SeekableByteChannel channel) throws IOException {
        // Given
        var inodeTable = new InodeTable(fileSystem, Block.Id.of(9), 120);
        var inodeId = Inode.Id.of(53);
        // When
        inodeTable.get(inodeId, channel);
        // Then
        verify(channel).position(anyLong());
        verify(channel).read(any(ByteBuffer.class));
        // When
        inodeTable.get(inodeId, channel);
        // Then
        verifyNoMoreInteractions(channel);
    }
    //endregion

    //region InodeTable::calcBlock
    @CsvSource({
            // inode | block size | first | expected
            "       1,         128,      0,       0",
            "       2,         128,      1,       2",
            "       1,         256,      0,       0",
            "       2,         256,      0,       0",
            "       3,         256,      0,       1",
            "       4,         256,      1,       2",
            "      32,        4096,    150,     150",
            "      33,        4096,    150,     151",
    })
    @ParameterizedTest
    void calcBlock_should_calculateBlockId(int inodeId, int blockSize, int firstBlock, int expectedResult) {
        // When
        var result = InodeTable.calcBlock(Inode.Id.of(inodeId), blockSize, Block.Id.of(firstBlock));
        // Then
        assertThat(result).isEqualTo(Block.Id.of(expectedResult));
    }
    //endregion

    //region InodeTable::calcPosition
    @CsvSource({
            // inode | block size | expected
            "       1,         128,       0",
            "       2,         128,       0",
            "       1,         256,       0",
            "       2,         256,     128",
            "       3,         256,       0",
            "       4,         256,     128",
            "      32,        4096,    3968",
            "      33,        4096,       0",
    })
    @ParameterizedTest
    void calcPosition_should_calculatePositionInsideBlock(int inodeId, int blockSize, int expectedResult) {
        // When
        var result = InodeTable.calcPosition(Inode.Id.of(inodeId), blockSize);
        // Then
        assertThat(result).isEqualTo(expectedResult);
    }
    //endregion

}

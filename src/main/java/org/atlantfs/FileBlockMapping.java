package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

class FileBlockMapping extends AbstractBlockMapping<DataBlock> implements FileOperations {

    private static final Logger log = Logger.getLogger(FileBlockMapping.class.getName());

    FileBlockMapping(Inode inode) {
        super(inode);
    }

    static FileBlockMapping read(Inode inode, ByteBuffer buffer) {
        return AbstractBlockMapping.read(inode, buffer, FileBlockMapping::new);
    }

    static FileBlockMapping init(Inode inode, Data data) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var result = new FileBlockMapping(inode);
        result.add(DataBlock.init(inode.getFileSystem(), data));
        result.dirty = true;
        return result;
    }

    @Override
    DataBlock readBlock(Block.Id id) {
        return DataBlock.read(fileSystem(), id);
    }

    @Override
    public void delete() throws IOException {
        super.delete();
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws DataOutOfMemoryException, BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var blockSize = blockSize();
        var lastExistingPosition = inode.getSize() - 1;
        var lastExistingBlockNumber = blocksCount - 1;
        var lastExistingBlock = get(lastExistingBlockNumber);
        var firstRequiredBlockNumber = (int) ((position + buffer.position()) / blockSize);
        if (lastExistingBlockNumber == firstRequiredBlockNumber) {
            // Need to fill with zeros all space from last written byte up to required start position
            var requiredOffset = (int) ((position + buffer.position()) % blockSize);
            if (!lastExistingBlock.hasData()) {
                if (requiredOffset > 0) {
                    lastExistingBlock.write(0, ByteBuffer.allocate(requiredOffset));
                }
            } else {
                var offset = (int) (lastExistingPosition % blockSize);
                var length = requiredOffset - offset;
                if (length > 0) {
                    lastExistingBlock.write(offset, ByteBuffer.allocate(length));
                }
            }
        }
        if (lastExistingBlockNumber < firstRequiredBlockNumber) {
            // There is a gap on end, then possibly N empty blocks and new block with gap on start.
            // Fill lastExistingBlockNumber by zeros up to the end
            {
                if (!lastExistingBlock.hasData()) {
                    lastExistingBlock.write(0, ByteBuffer.allocate(blockSize));
                } else {
                    var offset = (int) (lastExistingPosition % blockSize) + 1;
                    var length = blockSize - offset;
                    if (length > 0) {
                        lastExistingBlock.write(offset, ByteBuffer.allocate(length));
                    }
                }
            }
            for (int i = 0; i < firstRequiredBlockNumber - lastExistingBlockNumber - 1; i++) {
                add(DataBlock.init(fileSystem()));
            }
            {
                var dataBlock = DataBlock.init(fileSystem());
                var offset = (int) position % blockSize;
                if (offset > 0) {
                    dataBlock.write(0, ByteBuffer.allocate(offset));
                }
                add(dataBlock);
            }
        }
        var totalWritten = 0;
        DataBlock dataBlock;
        while (buffer.hasRemaining()) {
            var positionPlus = position + buffer.position();
            var blockNumber = (int) (positionPlus / blockSize);
            var offset = (int) (positionPlus % blockSize);
            var length = Math.min(blockSize - offset, buffer.remaining());
            if (blockNumber > blocksCount - 1) {
                dataBlock = DataBlock.init(fileSystem());
                add(dataBlock);
            } else {
                dataBlock = get(blockNumber);
            }
            var slice = buffer.slice(buffer.position(), length);
            var written = dataBlock.write(offset, slice);
            totalWritten += written;
            buffer.position(buffer.position() + written);
            if (written < length) {
                break;
            }
        }
        assert !buffer.hasRemaining();
        return totalWritten;
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        var blockSize = blockSize();
        var totalRead = 0;
        while (buffer.hasRemaining()) {
            var positionPlus = position + buffer.position();
            if (positionPlus >= inode.getSize()) {
                return totalRead;
            }
            var blockNumber = (int) Long.divideUnsigned(positionPlus, blockSize);
            var offset = (int) Long.remainderUnsigned(positionPlus, blockSize);
            var dataBlock = get(blockNumber);
            totalRead += dataBlock.read(offset, buffer);
        }
        return totalRead;
    }

}

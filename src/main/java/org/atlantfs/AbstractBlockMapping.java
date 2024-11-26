package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

abstract class AbstractBlockMapping<B extends Block> implements IBlock {

    private static final Logger log = Logger.getLogger(AbstractBlockMapping.class.getName());

    protected final AtlantFileSystem fileSystem;
    protected final List<Block.Pointer<B>> directs = new ArrayList<>();
    protected final List<Block.Pointer<IndirectBlock<B>>> indirects = new ArrayList<>();
    protected int blocksCount;
    protected boolean dirty;
    protected final List<Block> dirtyBlocks = new ArrayList<>();

    public AbstractBlockMapping(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    static <B extends Block, M extends AbstractBlockMapping<B>> M read(AtlantFileSystem fileSystem, ByteBuffer buffer, Function<AtlantFileSystem, M> factory) {
        var result = factory.apply(fileSystem);
        var position = buffer.position();
        var numberOfDirectBlocks = numberOfDirectBlocks(fileSystem.inodeSize());
        while (buffer.hasRemaining()) {
            var value = Block.Id.read(buffer);
            if (value.equals(Block.Id.ZERO)) {
                break;
            }
            if (result.directs.size() < numberOfDirectBlocks) {
                result.directs.add(Block.Pointer.of(value, result::readBlock));
            } else {
                var depth = result.indirects.size();
                var pointer = Block.Pointer.of(value, id -> result.readIndirectBlock(id, depth));
                result.indirects.add(pointer);
            }
        }
        var directBlocksCount = result.directs.size();
        var indirectBlocksCount = result.indirects.stream()
                .map(Block.Pointer::get)
                .mapToInt(IndirectBlock::size)
                .sum();
        result.blocksCount = directBlocksCount + indirectBlocksCount;
        buffer.position(position + fileSystem.iblockSize());
        return result;
    }

    @Override
    public void flush(ByteBuffer buffer) {
        var iblockLength = fileSystem.iblockSize();
        assert buffer.remaining() >= iblockLength;
        var initial = buffer.position();
        directs.forEach(id -> id.flush(buffer));
        indirects.forEach(id -> id.flush(buffer));
        if (buffer.hasRemaining()) {
            Block.Id.ZERO.write(buffer);
        }
        buffer.position(initial + iblockLength);
        dirtyBlocks.forEach(Block::flush);
        dirtyBlocks.clear();
    }

    B get(int blockNumber) {
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + "]...");
        if (blockNumber < directs.size()) {
            var result = directs.get(blockNumber).get();
            log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by direct");
            return result;
        }
        int index = blockNumber - directs.size();
        for (var pointer : indirects) {
            var indirectBlock = pointer.get();
            if (index < indirectBlock.maxSize()) {
                var result = indirectBlock.get(index);
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return result;
            }
            index -= indirectBlock.maxSize();
        }
        throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount() + "]");
    }

    void add(B block) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var blockNumber = blocksCount;
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + "]...");
        if (blockNumber < numberOfDirectBlocks(inodeSize())) {
            directs.add(Block.Pointer.of(block, this::readBlock));
            dirtyBlocks.add(block);
            blocksCount++;
            log.fine(() -> "Successfully added [blockId=" + block.id() + ", blockNumber=" + blockNumber + "] by direct");
            return;
        }
        int index = blockNumber - directs.size();
        for (int i = 0; i < numberOfIndirectLevels(); i++) {
            if (indirects.size() <= i) {
                var indirectBlock = IndirectBlock.init(fileSystem, i, this::readBlock, block);
                assert index < indirectBlock.maxSize();
                int finalI = i;
                indirects.add(Block.Pointer.of(indirectBlock, id -> readIndirectBlock(id, finalI)));
                dirtyBlocks.add(indirectBlock);
                blocksCount++;
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return;
            }
            var pointer = indirects.get(i);
            var indirectBlock = pointer.get();
            if (index < indirectBlock.maxSize()) {
                indirectBlock.add(block);
                dirtyBlocks.add(indirectBlock);
                blocksCount++;
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return;
            }
            index -= indirectBlock.maxSize();
        }
        throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount() + "]");
    }

    @Override
    public void delete() throws IOException {
        var directIds = directs.stream()
                .map(Block.Pointer::id)
                .toList();
        fileSystem.freeBlocks(directIds);
        indirects.stream().map(Block.Pointer::get).forEach(IndirectBlock::delete);
    }

    abstract B readBlock(Block.Id id);

    IndirectBlock<B> readIndirectBlock(Block.Id blockId, int depth) {
        return IndirectBlock.read(fileSystem, blockId, depth, this::readBlock);
    }

    protected int inodeSize() {
        return fileSystem.inodeSize();
    }

    protected int blockSize() {
        return fileSystem.blockSize();
    }

    @Override
    public int blocksCount() {
        return blocksCount;
    }

    @Override
    public long size() {
        return (long) blocksCount * fileSystem.blockSize();
    }

    static int numberOfDirectBlocks(int inodeSize) {
        return (inodeSize - Inode.MIN_LENGTH) / Block.Id.LENGTH - numberOfIndirectLevels();
    }

    static int numberOfIndirectLevels() {
        return 3;
    }

}

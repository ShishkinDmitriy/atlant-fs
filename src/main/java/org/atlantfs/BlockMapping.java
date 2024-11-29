package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

abstract class BlockMapping<B extends Block> implements Iblock {

    private static final Logger log = Logger.getLogger(BlockMapping.class.getName());

    protected final AtlantFileSystem fileSystem;
    protected final List<Block.Pointer<B>> directs = new ArrayList<>();
    protected final List<Block.Pointer<IndirectBlock<B>>> indirects = new ArrayList<>();
    protected int blocksCount;
    protected boolean dirty;
    protected final List<Block> dirtyBlocks = new ArrayList<>();

    public BlockMapping(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    static <B extends Block, M extends BlockMapping<B>> M read(AtlantFileSystem fileSystem, ByteBuffer buffer, Function<AtlantFileSystem, M> factory) {
        var result = factory.apply(fileSystem);
        var position = buffer.position();
        var numberOfDirectBlocks = numberOfDirectBlocks(fileSystem.inodeSize());
        while (buffer.hasRemaining()) {
            var value = Block.Id.read(buffer);
            if (value.equals(Block.Id.ZERO)) {
                break;
            }
            if (result.directs.size() < numberOfDirectBlocks) {
                result.directs.add(Block.Pointer.of(value));
            } else {
                result.indirects.add(Block.Pointer.of(value));
            }
        }
        var directBlocksCount = result.directs.size();
        var indirectBlocksCount = 0;
        for (int i = 0; i < result.indirects.size(); i++) {
            var finalI = i;
            var indirectBlock = result.indirects.get(i).computeIfAbsent(id -> result.readIndirectBlock(id, finalI));
            indirectBlocksCount += indirectBlock.size();
        }
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
            var result = directs.get(blockNumber).computeIfAbsent(this::readBlock);
            log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by direct");
            return result;
        }
        int index = blockNumber - directs.size();
        for (int i = 0; i < indirects.size(); i++) {
            int finalI = i;
            var indirectBlock = indirects.get(i).computeIfAbsent(id -> this.readIndirectBlock(id, finalI));
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
            directs.add(Block.Pointer.of(block));
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
                indirects.add(Block.Pointer.of(indirectBlock));
                dirtyBlocks.add(indirectBlock);
                blocksCount++;
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return;
            }
            int finalI1 = i;
            var indirectBlock = indirects.get(i).computeIfAbsent(id -> this.readIndirectBlock(id, finalI1));
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
        for (int i = 0; i < indirects.size(); i++) {
            Block.Pointer<IndirectBlock<B>> pointer = indirects.get(i);
            int finalI1 = i;
            IndirectBlock<?> block = pointer.computeIfAbsent(id -> this.readIndirectBlock(id, finalI1));
            block.delete();
        }
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

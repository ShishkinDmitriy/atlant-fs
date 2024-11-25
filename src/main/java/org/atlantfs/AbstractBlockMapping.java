package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

abstract class AbstractBlockMapping<B extends Block> implements IBlock {

    private static final Logger log = Logger.getLogger(AbstractBlockMapping.class.getName());

    protected final Inode inode;
    protected final List<Block.Pointer<B>> directs = new ArrayList<>();
    protected final List<Block.Pointer<IndirectBlock<B>>> indirects = new ArrayList<>();
    protected int blocksCount;
    protected boolean dirty;

    public AbstractBlockMapping(Inode inode) {
        this.inode = inode;
    }

    static <B extends Block, M extends AbstractBlockMapping<B>> M read(Inode inode, ByteBuffer buffer, Function<Inode, M> factory) {
        var result = factory.apply(inode);
        var position = buffer.position();
        var numberOfDirectBlocks = numberOfDirectBlocks(inode.inodeSize());
        while (buffer.hasRemaining()) {
            var value = Block.Id.read(buffer);
            if (value.equals(Block.Id.ZERO)) {
                return result;
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
        buffer.position(position + Inode.iBlockLength(inode.inodeSize()));
        return result;
    }

    @Override
    public void write(ByteBuffer buffer) {
        //noinspection resource
        var iblockLength = Inode.iBlockLength(fileSystem().inodeSize());
        assert buffer.remaining() >= iblockLength;
        var initial = buffer.position();
        directs.forEach(id -> id.write(buffer));
        indirects.forEach(id -> id.write(buffer));
        buffer.position(initial + iblockLength);
    }

    B get(int blockNumber) {
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + ", inodeId=" + inode.getId() + "]...");
        if (blockNumber < directs.size()) {
            var result = directs.get(blockNumber).get();
            log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by direct");
            return result;
        }
        int index = blockNumber - directs.size();
        for (var pointer : indirects) {
            var indirectBlock = pointer.get();
            if (index < indirectBlock.maxSize()) {
                var result = indirectBlock.get(blockNumber);
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return result;
            }
            index -= indirectBlock.maxSize();
        }
        throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount() + "]");
    }

    void add(B block) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var blockNumber = blocksCount();
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + ", inodeId=" + inode.getId() + "]...");
        if (blockNumber < numberOfDirectBlocks(inodeSize())) {
            directs.add(Block.Pointer.of(block, this::readBlock));
            blocksCount++;
            log.fine(() -> "Successfully added [blockId=" + block.id() + ", blockNumber=" + blockNumber + "] by direct");
            return;
        }
        int index = blockNumber - directs.size();
        for (var pointer : indirects) {
            var indirectBlock = pointer.get();
            if (index < indirectBlock.maxSize()) {
                var result = indirectBlock.add(block);
                blocksCount++;
                log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + result + "] by [" + indirectBlock.depth() + 1 + "] level indirect");
                return;
            }
            index -= indirectBlock.maxSize();
        }
        throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount() + "]");
    }

    protected void delete() throws IOException {
        var directIds = directs.stream()
                .map(Block.Pointer::id)
                .toList();
        //noinspection resource
        fileSystem().freeBlocks(directIds);
        indirects.stream().map(Block.Pointer::get).forEach(IndirectBlock::delete);
    }

    abstract B readBlock(Block.Id id);

    IndirectBlock<B> readIndirectBlock(Block.Id blockId, int depth) {
        return IndirectBlock.read(fileSystem(), blockId, depth, this::readBlock);
    }

    protected AtlantFileSystem fileSystem() {
        return inode.getFileSystem();
    }

    protected int inodeSize() {
        return inode.inodeSize();
    }

    protected int blockSize() {
        return inode.blockSize();
    }

    protected int blocksCount() {
        return inode.blocksCount();
    }

    static int numberOfDirectBlocks(int inodeSize) {
        return Inode.iBlockLength(inodeSize) / Block.Id.LENGTH - numberOfIndirectLevels();
    }

    static int numberOfIndirectLevels() {
        return 3;
    }

}

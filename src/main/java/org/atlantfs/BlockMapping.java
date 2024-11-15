package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

class BlockMapping implements DirectoryOperations, IBlock {

    private static final Logger log = Logger.getLogger(BlockMapping.class.getName());

    private final List<Block.Id> addresses;
    private final Inode inode;

    BlockMapping(Inode inode, List<Block.Id> addresses) {
        this.inode = inode;
        this.addresses = addresses;
    }

    static BlockMapping read(Inode inode, ByteBuffer buffer) {
        var numberOfAddresses = numberOfAddresses(inode.blockSize(), inode.inodeSize(), inode.blocksCount());
        var position = buffer.position();
        assert buffer.remaining() >= numberOfAddresses * Block.Id.LENGTH : "Buffer should have at least [" + numberOfAddresses + "] Block.Id values, but has only [" + buffer.remaining() + "] bytes";
        var blocks = new ArrayList<Block.Id>();
        for (var i = 0; i < numberOfAddresses; i++) {
            blocks.add(Block.Id.read(buffer));
        }
        buffer.position(position + Inode.iBlockLength(inode.fileSystem.inodeSize()));
        return new BlockMapping(inode, blocks);
    }

    static BlockMapping init(Inode inode, Block.Id blockId) {
        var blocks = new ArrayList<Block.Id>();
        blocks.add(blockId);
        return new BlockMapping(inode, blocks);
    }

    @Override
    public void write(ByteBuffer buffer) {
        var iBlockLength = Inode.iBlockLength(inode.fileSystem.inodeSize());
        assert buffer.remaining() >= iBlockLength;
        var initial = buffer.position();
        addresses.forEach(id -> id.write(buffer));
        buffer.position(initial + iBlockLength);
    }

    @Override
    public Iterator<DirEntry> iterator() {
        return new Iterator<>() {

            int currentList = 0;
            Iterator<DirEntry> currentIterator = readDirEntryList(resolve(currentList)).iterator();

            @Override
            public boolean hasNext() {
                var currentHasNext = currentIterator.hasNext();
                if (currentHasNext) {
                    return true;
                }
                if (currentList + 1 < inode.blocksCount()) {
                    currentList++;
                    currentIterator = readDirEntryList(resolve(currentList)).iterator();
                    return currentIterator.hasNext();
                }
                return false;
            }

            @Override
            public DirEntry next() {
                return currentIterator.next();
            }

        };
    }

    @Override
    public DirEntry add(Inode.Id inodeId, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        for (int i = 0; i < blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                var add = entryList.add(inodeId, fileType, name);
                inode.writeBlock(blockId, entryList::write);
                return add;
            } catch (DirectoryOutOfMemoryException _) {
                // continue
            }
        }
        var reserved = allocate();
        var dirEntryList = new DirEntryList(blockSize());
        var add = dirEntryList.add(inodeId, fileType, name);
        inode.writeBlock(reserved, dirEntryList::write);
        return add;
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                return entryList.get(name);
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirectoryOutOfMemoryException {
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                entryList.rename(name, newName);
                inode.writeBlock(blockId, entryList::write);
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                entryList.delete(name);
                inode.writeBlock(blockId, entryList::write);
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    Block.Id resolve(int blockNumber) {
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + ", inodeId=" + inode.getId() + ", addresses=" + addresses + "]...");
        var blocksCount = blocksCount();
        var inodeSize = inodeSize();
        var blockSize = blockSize();
        if (blockNumber >= blocksCount) {
            throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount + "]");
        }
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        if (blockNumber < numberOfDirectBlocks) {
            var blockId = addresses.get(blockNumber);
            log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + blockId + "] by direct");
            return blockId;
        }
        var level = addressLevel(blockSize, inodeSize, blockNumber);
        var offsets = addressOffsets(blockSize, inodeSize, blockNumber);
        assert addresses.size() >= numberOfDirectBlocks + level;
        var blockId = addresses.get(numberOfDirectBlocks - 1 + level);
        for (var offset : offsets) {
            blockId = readBlockId(blockId, offset);
        }
        Block.Id finalBlockId = blockId;
        log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + finalBlockId + "] by [" + level + "] level indirect");
        return blockId;
    }

    Block.Id allocate() throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        checkInvariant();
        var blockSize = blockSize();
        var inodeSize = inodeSize();
        var blocksCount = blocksCount();
        log.finer(() -> "Allocating new block [blockNumber=" + blocksCount + ", inodeId=" + inode.getId() + ", addresses=" + addresses + "]...");
        if (!canGrow(blockSize, inodeSize, blocksCount)) {
            throw new DirectoryOutOfMemoryException("Not enough memory to allocate new block");
        }
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        var offsets = addressOffsets(blockSize, inodeSize, blocksCount);
        var newIndirectBlocksCount = numberOfTrailingZeros(offsets);
        var reserved = inode.reserveBlocks(newIndirectBlocksCount + 1);
        var ids = flat(reserved);
        for (int i = ids.size() - 2; i >= 0; i--) {
            writeBlockId(ids.get(i), 0, ids.get(i + 1));
        }
        var result = ids.getLast();
        if (offsets.size() == newIndirectBlocksCount) {
            addresses.add(ids.getFirst());
            log.fine(() -> "Successfully allocated [blockNumber=" + blocksCount + ", blockId=" + result + "] by [" + offsets.size() + "] level indirect");
            return result;
        }
        var current = addresses.get(numberOfDirectBlocks + offsets.size() - 1);
        for (int i = 0; i < offsets.size() - newIndirectBlocksCount - 1; i++) {
            current = readBlockId(current, offsets.get(i));
        }
        writeBlockId(current, offsets.getLast(), result);
        checkInvariant();
        return result;
    }

    private static int numberOfTrailingZeros(List<Integer> offsets) {
        var result = 0;
        var cursor = offsets.size() - 1;
        while (cursor >= 0 && offsets.get(cursor) == 0) {
            result++;
            cursor--;
        }
        return result;
    }

    List<Block.Id> flat(List<Block.Range> ranges) {
        return ranges.stream()
                .flatMap(range -> IntStream.range(0, range.length())
                        .mapToObj(i -> range.from().plus(i)))
                .toList();
    }

    Block.Id readBlockId(Block.Id blockId, int offset) {
        log.fine(() -> "Reading from [blockId=" + blockId + ", offset=" + offset + "]...");
        var buffer = inode.readBlock(blockId);
        buffer.position(offset * Block.Id.LENGTH);
        return Block.Id.read(buffer);
    }

    void writeBlockId(Block.Id blockId, int offset, Block.Id value) {
        log.fine(() -> "Writing into [blockId=" + blockId + ", offset=" + offset + ", value=" + value + "]...");
        inode.writeBlock(blockId, offset * Block.Id.LENGTH, value::write);
    }

    DirEntryList readDirEntryList(Block.Id blockId) {
        var buffer = inode.readBlock(blockId);
        return DirEntryList.read(buffer);
    }

    private int blockSize() {
        return inode.blockSize();
    }

    private int inodeSize() {
        return inode.inodeSize();
    }

    private int blocksCount() {
        return inode.blocksCount();
    }

    private void checkInvariant() {
        assert addresses.size() == numberOfAddresses(blockSize(), inodeSize(), blocksCount());
    }

    static int addressLevel(int blockSize, int inodeSize, long blockNumber) {
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        if (blockNumber < numberOfDirectBlocks) {
            return 0;
        }
        blockNumber -= numberOfDirectBlocks;
        var numberOfIdsPerBlock = numberOfIdsPerBlock(blockSize);
        int result = 0;
        do {
            result++;
            blockNumber -= (int) Math.pow(numberOfIdsPerBlock, result);
        } while (blockNumber >= 0);
        return result;
    }

    static List<Integer> addressOffsets(int blockSize, int inodeSize, int blockNumber) {
        var level = addressLevel(blockSize, inodeSize, blockNumber);
        if (level == 0) {
            return new LinkedList<>();
        }
        blockNumber -= numberOfDirectBlocks(inodeSize);
        var numberOfIdsPerBlock = numberOfIdsPerBlock(blockSize);
        for (int i = 1; i < level; i++) {
            blockNumber -= (int) Math.pow(numberOfIdsPerBlock, i);
        }
        var result = new LinkedList<Integer>();
        for (int i = 0; i < level; i++) {
            result.addFirst(blockNumber % numberOfIdsPerBlock);
            blockNumber /= numberOfIdsPerBlock;
        }
        return result;
    }

    static boolean canGrow(int blockSize, int inodeSize, int blocksCount) {
        var futureNumberOfAddresses = numberOfAddresses(blockSize, inodeSize, blocksCount + 1);
        var maxNumberOfAddresses = numberOfDirectBlocks(inodeSize) + numberOfIndirectLevels();
        return futureNumberOfAddresses <= maxNumberOfAddresses;
    }

    static int numberOfAddresses(int blockSize, int inodeSize, int blocksCount) {
        return Math.min(blocksCount, numberOfDirectBlocks(inodeSize)) + addressLevel(blockSize, inodeSize, blocksCount - 1);
    }

    static int numberOfDirectBlocks(int inodeSize) {
        return Inode.iBlockLength(inodeSize) / Block.Id.LENGTH - numberOfIndirectLevels();
    }

    static int numberOfIdsPerBlock(int blockSize) {
        return blockSize / Block.Id.LENGTH;
    }

    static int numberOfIndirectLevels() {
        return 3;
    }

    List<Block.Id> getAddresses() {
        return Collections.unmodifiableList(addresses);
    }

}

package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class BlockMapping implements DirectoryOperations, IBlock {

    private final List<Block.Id> addresses;
    private final Inode inode;

    private BlockMapping(Inode inode, List<Block.Id> addresses) {
        this.inode = inode;
        this.addresses = addresses;
    }

    static BlockMapping read(Inode inode, ByteBuffer buffer) {
        var numberOfAddresses = numberOfAddresses(inode);
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

    private static int numberOfAddresses(Inode inode) {
        var blocksCount = inode.getBlocksCount();
        if (blocksCount <= numberOfDirectBlocks()) {
            return blocksCount;
        } else {
            return blocksCount + indirectLevel(blocksCount - numberOfDirectBlocks(), inode.blockSize());
        }
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
                if (currentList + 1 < inode.getBlocksCount()) {
                    currentList++;
                    currentIterator = readDirEntryList(resolve(currentList)).iterator();
                    return true;
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
        for (int i = 0; i < inode.getBlocksCount(); i++) {
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
        if (!canGrow()) {
            throw new DirectoryOutOfMemoryException("Not enough memory to create new Directory");
        }
        var reserved = inode.reserveBlock();
        addresses.add(reserved); // TODO: Handle indirect
        var dirEntryList = new DirEntryList(inode.blockSize());
        var add = dirEntryList.add(inodeId, fileType, name);
        inode.writeBlock(reserved, dirEntryList::write);
        return add;
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        for (int i = 0; i < inode.getBlocksCount(); i++) {
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
        for (int i = 0; i < inode.getBlocksCount(); i++) {
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
        for (int i = 0; i < inode.getBlocksCount(); i++) {
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
        if (blockNumber < numberOfDirectBlocks()) {
            return addresses.get(blockNumber);
        }
        blockNumber -= numberOfDirectBlocks();
        var blockSize = inode.blockSize();
        var level = indirectLevel(blockNumber, blockSize);
        assert addresses.size() >= numberOfDirectBlocks() + level;
        var blockId = addresses.get(numberOfDirectBlocks() + level - 1);
        for (var offset : indirectOffsets(level, blockNumber, blockSize)) {
            blockId = readBlockId(blockId, offset);
        }
        return blockId;
    }

    void allocate() {
        var blockNumber = inode.getBlocksCount() + 1;
        if (blockNumber >= numberOfDirectBlocks()) {
            blockNumber -= numberOfDirectBlocks();
            var level = indirectLevel(blockNumber, inode.blockSize());
            var offsets = indirectOffsets(level, blockNumber, inode.blockSize());
            for (var i = 0; i < offsets.size(); i++) {
                var last = offsets.pollLast();
//                if () {
//
//                }
            }
        }
    }

    Block.Id readBlockId(Block.Id blockId, int offset) {
        var buffer = inode.readBlock(blockId);
        buffer.position(offset);
        return Block.Id.read(buffer);
    }

    static int indirectLevel(long blockNumber, int blockSize) {
        var idsPerBlock = idsPerBlock(blockSize);
        int i = 0;
        while (blockNumber >= 0) {
            i++;
            blockNumber -= (int) Math.pow(idsPerBlock, i);
        }
        return i;
    }

    static Deque<Integer> indirectOffsets(int level, int blockNumber, int blockSize) {
        var result = new LinkedList<Integer>();
        var current = blockNumber;
        for (int i = 0; i < level; i++) {
            result.addFirst(current % idsPerBlock(blockSize));
            current /= idsPerBlock(blockSize);
        }
        return result;
    }

    private DirEntryList readDirEntryList(Block.Id blockId) {
        var buffer = inode.readBlock(blockId);
        return DirEntryList.read(buffer);
    }

    private boolean canGrow() {
        return inode.getBlocksCount() < numberOfDirectBlocks() + (long) getIndirectBlocks() * inode.blockSize();
    }

    static int numberOfDirectBlocks() {
        return 7;
    }

    static int getIndirectBlocks() {
        return 1;
    }

    static int idsPerBlock(int blockSize) {
        return blockSize / Block.Id.LENGTH;
    }

}

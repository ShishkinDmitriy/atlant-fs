package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class BlockMapping implements DirectoryOperations {

    private final List<Block.Id> addresses = new ArrayList<>();
    private final Inode inode;

    BlockMapping(Inode inode) {
        this.inode = inode;
    }

    static BlockMapping read(Inode inode, ByteBuffer buffer) {
        var blocks = new ArrayList<Block.Id>();
        for (int i = 0; i < Math.min(inode.getBlocksCount(), getDirectBlocks() + getDirectBlocks()); i++) {
            Block.Id read = Block.Id.read(buffer);
            blocks.add(read);
        }
        var blockList = new BlockMapping(inode);
        blockList.getAddresses().addAll(blocks);
        return blockList;
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
                if (currentList < inode.getBlocksCount()) {
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
        if (blockNumber < getDirectBlocks()) {
            return addresses.get(blockNumber);
        }
        blockNumber -= getDirectBlocks();
        var level = indirectLevel(blockNumber);
        assert addresses.size() >= getDirectBlocks() + level;
        var blockId = addresses.get(getDirectBlocks() + level - 1);
        for (var offset : indirectOffsets(level, blockNumber)) {
            blockId = readBlockId(blockId, offset);
        }
        return blockId;
    }

    void allocate() {
        var blockNumber = inode.getBlocksCount() + 1;
        if (blockNumber >= getDirectBlocks()) {
            blockNumber -= getDirectBlocks();
            var level = indirectLevel(blockNumber);
            var offsets = indirectOffsets(level, blockNumber);
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

    int indirectLevel(long blockNumber) {
        var idsPerBlock = idsPerBlock();
        int i = 0;
        while (blockNumber >= 0) {
            i++;
            blockNumber -= (int) Math.pow(idsPerBlock, i);
        }
        return i;
    }

    Deque<Integer> indirectOffsets(int level, int blockNumber) {
        var result = new LinkedList<Integer>();
        var current = blockNumber;
        for (int i = 0; i < level; i++) {
            result.addFirst(current % idsPerBlock());
            current /= idsPerBlock();
        }
        return result;
    }

    private DirEntryList readDirEntryList(Block.Id blockId) {
        var buffer = inode.readBlock(blockId);
        return DirEntryList.read(buffer);
    }

    private boolean canGrow() {
        return inode.getBlocksCount() < getDirectBlocks() + (long) getIndirectBlocks() * inode.blockSize();
    }

    static int getDirectBlocks() {
        return 7;
    }

    static int getIndirectBlocks() {
        return 1;
    }

    int idsPerBlock() {
        return inode.blockSize() / Block.Id.LENGTH;
    }

    public List<Block.Id> getAddresses() {
        return addresses;
    }

}

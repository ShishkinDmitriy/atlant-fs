package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class BlockList implements DirectoryOperations {

    private final List<Block.Id> addresses = new ArrayList<>();
    private final Inode inode;

    BlockList(Inode inode) {
        this.inode = inode;
    }

    static BlockList read(Inode inode, ByteBuffer buffer) {
        var blocks = new ArrayList<Block.Id>();
        for (int i = 0; i < Math.min(inode.getBlocksCount(), getDirectBlocks() + getDirectBlocks()); i++) {
            Block.Id read = Block.Id.read(buffer);
            blocks.add(read);
        }
        var blockList = new BlockList(inode);
        blockList.getAddresses().addAll(blocks);
        return blockList;
    }

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
        if (blockNumber < getDirectBlocks() + idsPerBlock()) {
            assert addresses.size() == getDirectBlocks() + getIndirectBlocks();
            var indirectBlockId = addresses.getLast();
            var offset = (blockNumber - getDirectBlocks()) % idsPerBlock();
            var buffer = inode.readBlock(indirectBlockId);
            buffer.position(offset);
            return Block.Id.read(buffer);
        }
        throw new IllegalArgumentException("Too much");
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

package org.atlantfs;

import java.util.function.BiFunction;

class InodeTableRegion implements Region {

    private final AtlantFileSystem fileSystem;
    private final Cache<Inode.Id, Inode<?>> cache = new Cache<>();
    private final DirInode root;

    InodeTableRegion(AtlantFileSystem fileSystem) {
        this(fileSystem, null);
    }

    InodeTableRegion(AtlantFileSystem fileSystem, DirInode root) {
        this.fileSystem = fileSystem;
        if (root == null) {
            try {
                root = createDirectory();
                assert root.getId().equals(Inode.Id.ROOT) : "Expected ROOT id, but was [" + root.getId() + "]";
            } catch (BitmapRegion.NotEnoughSpaceException e) {
                throw new RuntimeException(e);
            }
        }
        this.root = root;
    }

    static InodeTableRegion read(AtlantFileSystem fileSystem) {
        var root = fileSystem.readInode(Inode.Id.ROOT);
        return new InodeTableRegion(fileSystem, (DirInode) root);
    }

    Inode<?> get(Inode.Id inodeId) {
        checkInodeIdLimit(inodeId);
        return cache.computeIfAbsent(inodeId, fileSystem::readInode);
    }

    FileInode createFile() throws BitmapRegion.NotEnoughSpaceException {
        return (FileInode) createInode(FileInode::init);
    }

    DirInode createDirectory() throws BitmapRegion.NotEnoughSpaceException {
        var reserved = fileSystem.reserveInode();
        checkInodeIdLimit(reserved);
        var result = DirInode.init(fileSystem, reserved);
        result.flush();
        cache.put(reserved, result);
        return result;
    }

    private Inode<?> createInode(BiFunction<AtlantFileSystem, Inode.Id, Inode> function) throws BitmapRegion.NotEnoughSpaceException {
        var reserved = fileSystem.reserveInode();
        checkInodeIdLimit(reserved);
        var inode = function.apply(fileSystem, reserved);
        fileSystem.writeInode(inode);
        cache.put(reserved, inode);
        return inode;
    }

    void delete(Inode.Id inodeId) {
        checkInodeIdLimit(inodeId);
        fileSystem.freeInode(inodeId);
        cache.remove(inodeId);
    }

    private void checkInodeIdLimit(Inode.Id inodeId) {
        if (inodeId.value() > maxInodeCount()) {
            throw new IndexOutOfBoundsException("Inode [" + inodeId + "] is out of bounds [" + maxInodeCount() + "]");
        }
    }

    private int maxInodeCount() {
        return numberOfBlocks() * (blockSize() / inodeSize());
    }

    Block.Id calcBlock(Inode.Id inodeId, int blockSize, Block.Id firstBlock) {
        return Block.Id.of(firstBlock.value() + (inodeId.value() - 1) * inodeSize() / blockSize);
    }

    int calcPosition(Inode.Id inodeId, int blockSize) {
        return (inodeId.value() - 1) * inodeSize() % blockSize;
    }

    @Override
    public int blockSize() {
        return fileSystem.blockSize();
    }

    @Override
    public Block.Id firstBlock() {
        return fileSystem.superBlock().firstBlockOfInodeTables();
    }

    @Override
    public int numberOfBlocks() {
        return fileSystem.superBlock().numberOfInodeTables();
    }

    int inodeSize() {
        return fileSystem.inodeSize();
    }

    DirInode root() {
        return root;
    }

}

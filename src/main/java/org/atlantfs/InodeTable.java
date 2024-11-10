package org.atlantfs;

class InodeTable implements AbstractRegion {

    private final AtlantFileSystem fileSystem;
    private final Cache<Inode.Id, Inode> cache = new Cache<>();
    private final Inode root;

    InodeTable(AtlantFileSystem fileSystem) {
        this(fileSystem, null);
    }

    InodeTable(AtlantFileSystem fileSystem, Inode root) {
        this.fileSystem = fileSystem;
        if (root == null) {
            try {
                root = createDirectory();
                assert root.getId().equals(Inode.Id.ROOT) : "Expected ROOT id, but was [" + root.getId() + "]";
            } catch (BitmapRegionOutOfMemoryException e) {
                throw new RuntimeException(e);
            }
        }
        this.root = root;
    }

    static InodeTable read(AtlantFileSystem fileSystem) {
        var buffer = fileSystem.readInode(Inode.Id.ROOT);
        var root = Inode.read(fileSystem, buffer, Inode.Id.ROOT);
        return new InodeTable(fileSystem, root);
    }

    Inode get(Inode.Id inodeId) {
        if (inodeId.value() > maxInodeCount()) {
            throw new IndexOutOfBoundsException("Inode [" + inodeId + "] is out of bounds [" + maxInodeCount() + "]");
        }
        return cache.computeIfAbsent(inodeId, id -> {
            var buffer = fileSystem.readInode(id);
            return Inode.read(fileSystem, buffer, id);
        });
    }

    public Inode createFile() throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveInode();
        var inode = Inode.createRegularFile(fileSystem, reserved);
        cache.put(reserved, inode);
        return inode;
    }

    public Inode createDirectory() throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveInode();
        var inode = Inode.createDirectory(fileSystem, reserved);
        cache.put(reserved, inode);
        return inode;
    }

    void delete(Inode.Id inodeId) {
        fileSystem.freeInode(inodeId);
        cache.remove(inodeId);
    }

    private int maxInodeCount() {
        return numberOfBlocks() * (blockSize() / Inode.LENGTH);
    }

    static Block.Id calcBlock(Inode.Id inodeId, int blockSize, Block.Id firstBlock) {
        return Block.Id.of(firstBlock.value() + (inodeId.value() - 1) * Inode.LENGTH / blockSize);
    }

    static int calcPosition(Inode.Id inodeId, int blockSize) {
        return (inodeId.value() - 1) * Inode.LENGTH % blockSize;
    }

    public int blockSize() {
        return fileSystem.getSuperBlock().getBlockSize();
    }

    public Block.Id firstBlock() {
        return fileSystem.getSuperBlock().getInodeTableFirstBlock();
    }

    public int numberOfBlocks() {
        return fileSystem.getSuperBlock().getInodeTablesNumberOfBlocks();
    }

    public Inode getRoot() {
        return root;
    }
}

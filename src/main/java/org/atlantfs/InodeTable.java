package org.atlantfs;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class InodeTable implements AbstractRegion {

    private final AtlantFileSystem fileSystem;
    private final Map<Inode.Id, SoftReference<Inode>> cache = new ConcurrentHashMap<>();

    InodeTable(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    Inode get(Inode.Id inodeId) {
        var toAdd = new HashMap<Inode.Id, SoftReference<Inode>>();
        var inodeSoftReference = cache.computeIfAbsent(inodeId, id -> {
            if (id.value() > maxInodeCount()) {
                throw new IllegalArgumentException("Out of bounds");
            }
            var firstBlock = firstBlock();
            var blockSize = blockSize();
            var blockId = calcBlock(id, blockSize, firstBlock);
            var buffer = fileSystem.readBlock(blockId);
            Inode result = null;
            var currentId = Inode.Id.of((blockSize / Inode.LENGTH) * (blockId.value() - firstBlock.value()));
            while (buffer.hasRemaining()) {
                var currentInode = Inode.read(buffer);
                if (currentId.equals(id)) {
                    result = currentInode;
                } else {
                    toAdd.put(currentId, new SoftReference<>(currentInode));
                }
                currentId = Inode.Id.of(currentId.value() + 1);
            }
            assert result != null;
            return new SoftReference<>(result);
        });
        cache.putAll(toAdd);
        return inodeSoftReference.get();
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

}

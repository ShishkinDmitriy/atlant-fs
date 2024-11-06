package org.atlantfs;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class InodeTable {

    private Inode rootInode;
    private final Block.Id firstBlock;
    private final int numberOfBlocks;
    private final int blockSize;
    private final Map<Inode.Id, SoftReference<Inode>> cache = new ConcurrentHashMap<>();

    InodeTable(AtlantFileSystem fileSystem, Block.Id firstBlock, int numberOfBlocks) {
        this.firstBlock = firstBlock;
        this.numberOfBlocks = numberOfBlocks;
        this.blockSize = fileSystem.getSuperBlock().getBlockSize();
    }

    Inode get(Inode.Id inodeId, SeekableByteChannel channel) {
        var toAdd = new HashMap<Inode.Id, SoftReference<Inode>>();
        var inodeSoftReference = cache.computeIfAbsent(inodeId, id -> {
            if (id.value() > maxInodeCount()) {
                throw new IllegalArgumentException("Out of bounds");
            }
            var blockId = calcBlock(id, blockSize, firstBlock);
            var buffer = ByteBuffer.allocate(blockSize);
            try {
                channel.position((long) blockId.value() * blockSize);
                channel.read(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e); // TODO
            }
            Inode result = null;
            var currentId = new Inode.Id((blockSize / Inode.LENGTH) * (blockId.value() - firstBlock.value()));
            while (buffer.hasRemaining()) {
                var currentInode = Inode.read(buffer);
                toAdd.put(currentId, new SoftReference<>(currentInode));
                if (currentId.equals(id)) {
                    result = currentInode;
                }
                currentId = new Inode.Id(currentId.value() + 1);
            }
            assert result != null;
            return new SoftReference<>(result);
        });
        cache.putAll(toAdd);
        return inodeSoftReference.get();
    }

    private int maxInodeCount() {
        return numberOfBlocks * (blockSize / Inode.LENGTH);
    }

    static Block.Id calcBlock(Inode.Id inodeId, int blockSize, Block.Id firstBlock) {
        return new Block.Id(firstBlock.value() + (inodeId.value() - 1) * Inode.LENGTH / blockSize);
    }

    static int calcPosition(Inode.Id inodeId, int blockSize) {
        return (inodeId.value() - 1) * Inode.LENGTH % blockSize;
    }

}

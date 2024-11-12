package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Optional;

import static org.atlantfs.AtlantFileSystem.BLOCK_SIZE;

final class SuperBlock {

    public static final int LENGTH = 2 + 2 + 4 + 4 + 4 + 4 + 4;
    public static final short MAGIC = (short) 0xEF54;

    private int blockSize;
    private int inodeSize;
    private int numberOfBlockBitmaps;
    private int numberOfInodeBitmaps;
    private int numberOfInodeTables;

    static SuperBlock read(ByteBuffer buffer) {
        short magic = buffer.getShort();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Bad magic for AtlantFS [" + magic + "]");
        }
        var _ = buffer.getShort();
        SuperBlock result = new SuperBlock();
        result.setBlockSize(buffer.getInt());
        result.setInodeSize(buffer.getInt());
        result.setNumberOfBlockBitmaps(buffer.getInt());
        result.setNumberOfInodeBitmaps(buffer.getInt());
        result.setNumberOfInodeTables(buffer.getInt());
        assert !buffer.hasRemaining();
        return result;
    }

    static SuperBlock withDefaults(Map<String, ?> env) {
        int blockSize = Optional.ofNullable(env.get(BLOCK_SIZE))
                .filter(Integer.class::isInstance)
                .map(Integer.class::cast)
                .orElseGet(SuperBlock::getUnderlyingBlockSize);
        var superBlock = new SuperBlock();
        superBlock.setBlockSize(blockSize);
        superBlock.setInodeSize(64);
        superBlock.setNumberOfBlockBitmaps(1);
        superBlock.setNumberOfInodeBitmaps(4);
        superBlock.setNumberOfInodeTables(64);
        return superBlock;
    }

    void write(ByteBuffer buffer) {
        buffer.putShort(MAGIC);
        buffer.putShort((short) 0);
        buffer.putInt(blockSize);
        buffer.putInt(inodeSize);
        buffer.putInt(numberOfBlockBitmaps);
        buffer.putInt(numberOfInodeBitmaps);
        buffer.putInt(numberOfInodeTables);
        assert buffer.position() == LENGTH;
    }

    int blockSize() {
        return blockSize;
    }

    int inodeSize() {
        return inodeSize;
    }

    int numberOfBlockBitmaps() {
        return numberOfBlockBitmaps;
    }

    int numberOfInodeBitmaps() {
        return numberOfInodeBitmaps;
    }

    int numberOfInodeTables() {
        return numberOfInodeTables;
    }

    Block.Id firstBlockOfBlockBitmap() {
        return Block.Id.of(1);
    }

    Block.Id firstBlockOfInodeBitmap() {
        return firstBlockOfBlockBitmap().plus(numberOfBlockBitmaps);
    }

    Block.Id firstBlockOfInodeTables() {
        return firstBlockOfInodeBitmap().plus(numberOfInodeBitmaps);
    }

    Block.Id firstBlockOfData() {
        return firstBlockOfInodeTables().plus(numberOfInodeTables);
    }

    private static int getUnderlyingBlockSize() {
        try {
            return Math.toIntExact(FileSystems.getDefault()
                    .getFileStores()
                    .iterator()
                    .next()
                    .getBlockSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //region private setters
    private void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    private void setInodeSize(int inodeSize) {
        this.inodeSize = inodeSize;
    }

    private void setNumberOfBlockBitmaps(int numberOfBlockBitmaps) {
        this.numberOfBlockBitmaps = numberOfBlockBitmaps;
    }

    private void setNumberOfInodeBitmaps(int numberOfInodeBitmaps) {
        this.numberOfInodeBitmaps = numberOfInodeBitmaps;
    }

    private void setNumberOfInodeTables(int numberOfInodeTables) {
        this.numberOfInodeTables = numberOfInodeTables;
    }
    //endregion

}

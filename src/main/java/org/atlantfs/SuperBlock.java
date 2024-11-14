package org.atlantfs;

import java.nio.ByteBuffer;

final class SuperBlock {

    public static final int LENGTH = 2 + 2 + 4 + 4 + 4 + 4 + 4;
    public static final short MAGIC = (short) 0xEF54;

    private int blockSize;
    private int inodeSize;
    private int numberOfBlockBitmaps;
    private int numberOfInodeBitmaps;
    private int numberOfInodeTables;

    private SuperBlock() {
    }

    static SuperBlock init(AtlantConfig atlantConfig) {
        var superBlock = new SuperBlock();
        superBlock.setBlockSize(atlantConfig.blockSize());
        superBlock.setInodeSize(atlantConfig.inodeSize());
        superBlock.setNumberOfBlockBitmaps(atlantConfig.numberOfBlockBitmaps());
        superBlock.setNumberOfInodeBitmaps(atlantConfig.numberOfInodeBitmaps());
        superBlock.setNumberOfInodeTables(atlantConfig.numberOfInodeTables());
        return superBlock;
    }

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

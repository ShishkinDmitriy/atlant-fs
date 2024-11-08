package org.atlantfs;

import java.nio.ByteBuffer;

final class SuperBlock {

    public static final int LENGTH = 2 + 2 + 4 + 4 + 4 + 4 + 4;
    public static final short MAGIC = (short) 0xEF54;

    private int blockSize;
    private int inodeSize;
    private int blockBitmapsNumber;
    private int inodeBitmapsNumber;
    private int inodeTablesNumber;

    static SuperBlock read(ByteBuffer buffer) {
        short magic = buffer.getShort();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Bad magic for AtlantFS [" + magic + "]");
        }
        var _ = buffer.getShort();
        SuperBlock result = new SuperBlock();
        result.setBlockSize(buffer.getInt());
        result.setInodeSize(buffer.getInt());
        result.setBlockBitmapsNumber(buffer.getInt());
        result.setInodeBitmapsNumber(buffer.getInt());
        assert !buffer.hasRemaining();
        return result;
    }

    void write(ByteBuffer buffer) {
        buffer.putShort(MAGIC);
        buffer.putShort((short) 0);
        buffer.putInt(blockSize);
        buffer.putInt(inodeSize);
        buffer.putInt(blockBitmapsNumber);
        buffer.putInt(inodeBitmapsNumber);
        assert buffer.position() == LENGTH;
    }

    Block.Id getBlockBitmapFirstBlock() {
        return Block.Id.of(1);
    }

    int getBlockBitmapNumberOfBlocks() {
        return blockBitmapsNumber;
    }

    Block.Id getInodeBitmapFirstBlock() {
        return Block.Id.of(getBlockBitmapFirstBlock().value() + getBlockBitmapNumberOfBlocks());
    }

    int getInodeBitmapNumberOfBlocks() {
        return inodeBitmapsNumber;
    }

    Block.Id getInodeTableFirstBlock() {
        return Block.Id.of(getInodeBitmapFirstBlock().value() + getInodeBitmapNumberOfBlocks());
    }

    int getInodeTablesNumberOfBlocks() {
        return inodeTablesNumber;
    }

    // -----------------------------------------------------------------------------------------------------------------

    int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public int getInodeSize() {
        return inodeSize;
    }

    public void setInodeSize(int inodeSize) {
        this.inodeSize = inodeSize;
    }

    public int getBlockBitmapsNumber() {
        return blockBitmapsNumber;
    }

    public void setBlockBitmapsNumber(int blockBitmapsNumber) {
        this.blockBitmapsNumber = blockBitmapsNumber;
    }

    public int getInodeBitmapsNumber() {
        return inodeBitmapsNumber;
    }

    public void setInodeBitmapsNumber(int inodeBitmapsNumber) {
        this.inodeBitmapsNumber = inodeBitmapsNumber;
    }

}

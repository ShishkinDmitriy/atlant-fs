package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;

final class SuperBlock implements Block {

    public static final int LENGTH = 2 + 2 + 4 + 4 + 4 + 4 + 4;
    public static final short MAGIC = (short) 0xEF54;

    private int blockSize;
    private int inodeSize;
    private int numberOfBlockBitmaps;
    private int numberOfInodeBitmaps;
    private int numberOfInodeTables;
    private boolean dirty;
    private final AtlantFileSystem fileSystem;

    private SuperBlock(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    static SuperBlock init(AtlantFileSystem fileSystem, AtlantConfig atlantConfig) {
        var result = new SuperBlock(fileSystem);
        result.setBlockSize(atlantConfig.blockSize());
        result.setInodeSize(atlantConfig.inodeSize());
        result.setNumberOfBlockBitmaps(atlantConfig.numberOfBlockBitmaps());
        result.setNumberOfInodeBitmaps(atlantConfig.numberOfInodeBitmaps());
        result.setNumberOfInodeTables(atlantConfig.numberOfInodeTables());
        result.dirty = true;
        return result;
    }

    static SuperBlock read(AtlantFileSystem fileSystem, ByteBuffer buffer) {
        short magic = buffer.getShort();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Bad magic for AtlantFS [" + magic + "]");
        }
        var _ = buffer.getShort();
        SuperBlock result = new SuperBlock(fileSystem);
        result.setBlockSize(buffer.getInt());
        result.setInodeSize(buffer.getInt());
        result.setNumberOfBlockBitmaps(buffer.getInt());
        result.setNumberOfInodeBitmaps(buffer.getInt());
        result.setNumberOfInodeTables(buffer.getInt());
        assert !buffer.hasRemaining();
        return result;
    }

    @Override
    public void flush() {
        if (!isDirty()) {
            return;
        }
        fileSystem.writeBlock(id(), buffer -> {
            buffer.putShort(MAGIC);
            buffer.putShort((short) 0);
            buffer.putInt(blockSize);
            buffer.putInt(inodeSize);
            buffer.putInt(numberOfBlockBitmaps);
            buffer.putInt(numberOfInodeBitmaps);
            buffer.putInt(numberOfInodeTables);
            assert buffer.position() == LENGTH;
        });
        dirty = false;
    }

    @Override
    public Id id() {
        return Id.ZERO;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public void delete() throws IOException {
        // Do nothing
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

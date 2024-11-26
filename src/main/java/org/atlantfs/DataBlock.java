package org.atlantfs;

import java.nio.ByteBuffer;

class DataBlock implements Block, FileOperations {

    private final Id id;
    private final Data data;
    private final AtlantFileSystem fileSystem;
    private boolean dirty;

    DataBlock(AtlantFileSystem fileSystem, Id id, Data data) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.data = data;
    }

    static DataBlock read(AtlantFileSystem fileSystem, Block.Id id) {
        var buffer = fileSystem.readBlock(id);
        var data = Data.read(buffer, fileSystem.blockSize());
        return new DataBlock(fileSystem, id, data);
    }

    static DataBlock init(AtlantFileSystem fileSystem) throws BitmapRegionOutOfMemoryException {
        var data = new Data(fileSystem.blockSize());
        return init(fileSystem, data);
    }

    static DataBlock init(AtlantFileSystem fileSystem, byte[] bytes) throws BitmapRegionOutOfMemoryException {
        var blockBytes = new byte[fileSystem.blockSize()];
        System.arraycopy(bytes, 0, blockBytes, 0, Math.min(bytes.length, blockBytes.length));
        var data = new Data(blockBytes, bytes.length);
        return init(fileSystem, data);
    }

    static DataBlock init(AtlantFileSystem fileSystem, Data data) throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlock();
        var dataBlock = new DataBlock(fileSystem, reserved, data);
        dataBlock.dirty = true;
        return dataBlock;
    }

    @Override
    public void flush() {
        if (!isDirty()) {
            return;
        }
        fileSystem.writeBlock(id, data::flush);
        dirty = false;
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DataOutOfMemoryException {
        return data.write(position, buffer);
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        return data.read(position, buffer);
    }

    @Override
    public void delete() {
        fileSystem.freeBlock(id);
    }

    @Override
    public Id id() {
        return id;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    int size() {
        return data.size();
    }

    byte[] bytes() {
        return data.bytes();
    }

    boolean hasData() {
        return data.hasData();
    }

}

package org.atlantfs;

import java.nio.ByteBuffer;

class DataIblock implements IBlock, FileOperations {

    private final Data data;

    DataIblock(Data data) {
        this.data = data;
    }

    static DataIblock read(ByteBuffer buffer, long size) {
        var data = Data.read(buffer, (int) size);
        return new DataIblock(data);
    }

    static DataIblock init(AtlantFileSystem fileSystem) {
        var data = new Data(fileSystem.iblockSize());
        return init(data);
    }

    static DataIblock init(AtlantFileSystem fileSystem, byte[] bytes) {
        var blockBytes = new byte[fileSystem.iblockSize()];
        System.arraycopy(bytes, 0, blockBytes, 0, Math.min(bytes.length, blockBytes.length));
        var data = new Data(blockBytes, bytes.length);
        return init(data);
    }

    static DataIblock init(Data data) {
        return new DataIblock(data);
    }

    @Override
    public void flush(ByteBuffer buffer) {
        data.flush(buffer);
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
    }

    @Override
    public IBlockType type() {
        return IBlockType.FILE_INLINE_DATA;
    }

    @Override
    public long size() {
        return data.size();
    }

    @Override
    public int blocksCount() {
        return 0;
    }

    public Data data() {
        return data;
    }
}

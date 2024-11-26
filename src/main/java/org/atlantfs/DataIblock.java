package org.atlantfs;

import java.nio.ByteBuffer;

class DataIblock implements FileIblock {

    private final Data data;

    DataIblock(Data data) {
        this.data = data;
    }

    static DataIblock read(AtlantFileSystem fileSystem, ByteBuffer buffer, long size) {
        var initial = buffer.position();
        var data = Data.read(buffer.slice(initial, fileSystem.iblockSize()), (int) size);
        buffer.position(initial + fileSystem.iblockSize());
        return new DataIblock(data);
    }

    static DataIblock init(AtlantFileSystem fileSystem) {
        var data = new Data(new byte[fileSystem.iblockSize()], 0);
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

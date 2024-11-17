package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;

class Data implements IBlock, FileOperations {

    private final byte[] data;
    private int length;

    Data(byte[] data, int length) {
        this.data = data;
        this.length = length;
    }

    static Data read(ByteBuffer buffer, int length) {
        byte[] array = new byte[buffer.remaining()];
        var i = 0;
        while (buffer.hasRemaining()) {
            array[i++] = buffer.get();
        }
        return new Data(array, length);
    }

    @Override
    public void write(ByteBuffer buffer) {
        assert buffer.remaining() >= data.length;
        buffer.put(data);
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException, DataOutOfMemoryException {
        var targetLength = position + buffer.remaining();
        if (targetLength > data.length) {
            throw new DataOutOfMemoryException();
        }
        var i = 0;
        while (buffer.hasRemaining()) {
            data[(int) (position + i++)] = buffer.get();
        }
        length = (int) targetLength;
        return i;
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        if (position >= length) {
            return 0;
        }
        var bound = Math.min(buffer.remaining(), length - (int) position);
        for (int i = 0; i < bound; i++) {
            buffer.put(data[(int) (position + i)]);
        }
        return bound;
    }

    @Override
    public void delete() throws DirectoryNotEmptyException {

    }

    boolean hasData() {
        return length > 0;
    }

}

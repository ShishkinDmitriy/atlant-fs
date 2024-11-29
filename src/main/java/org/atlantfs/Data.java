package org.atlantfs;

import java.nio.ByteBuffer;

class Data implements FileOperations {

    private final byte[] data; // Has size of block
    private int length; // Less than block size, how many is used

    Data(byte[] data, int length) {
        this.data = data;
        this.length = length;
        checkInvariant();
    }

    static Data init(int capacity) {
        return new Data(new byte[capacity], 0);
    }

    static Data read(ByteBuffer buffer, int length) {
        byte[] array = new byte[buffer.remaining()];
        var i = 0;
        while (buffer.hasRemaining()) {
            array[i++] = buffer.get();
            if (i == length) {
                break;
            }
        }
        return new Data(array, length);
    }

    void flush(ByteBuffer buffer) {
        assert buffer.remaining() >= data.length;
        buffer.put(data);
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        var targetLength = position + buffer.remaining();
        if (targetLength > data.length) {
            throw new DataOutOfMemoryException();
        }
        var i = 0;
        while (buffer.hasRemaining()) {
            data[(int) (position + i++)] = buffer.get();
        }
        length = (int) targetLength;
        checkInvariant();
        return i;
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        if (position >= length) {
            return 0;
        }
        var bound = Math.min(buffer.remaining(), data.length - (int) position);
        for (int i = 0; i < bound; i++) {
            buffer.put(data[(int) (position + i)]);
        }
        return bound;
    }

    int size() {
        return length;
    }

    byte[] bytes() {
        var result = new byte[length];
        System.arraycopy(data, 0, result, 0, length);
        return result;
    }

    boolean hasData() {
        return length > 0;
    }

    void checkInvariant() {
        assert data != null;
        assert data.length >= length;
    }

}

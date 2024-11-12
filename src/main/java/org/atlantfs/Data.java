package org.atlantfs;

import java.nio.ByteBuffer;

class Data implements IBlock {

    private final byte[] data;
    private final int length;

    public Data(byte[] data, int length) {
        this.data = data;
        this.length = length;
    }

    static Data read(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        var i = 0;
        while (buffer.hasRemaining()) {
            array[i++] = buffer.get();
        }
        assert !buffer.hasRemaining();
        var data1 = new Data(array, array.length);
        return data1;
    }

    @Override
    public void write(ByteBuffer buffer) {
        assert buffer.remaining() == length;
        buffer.put(data);
        assert !buffer.hasRemaining();
    }

}

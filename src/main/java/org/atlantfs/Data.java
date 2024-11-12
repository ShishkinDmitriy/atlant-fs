package org.atlantfs;

import java.nio.ByteBuffer;

class Data {

    private final byte[] data;
    private final int length;

    public Data(byte[] data, int length) {
        this.data = data;
        this.length = length;
    }

    static Data read(ByteBuffer buffer) {
        return new Data(buffer.array(), buffer.remaining());
    }

}

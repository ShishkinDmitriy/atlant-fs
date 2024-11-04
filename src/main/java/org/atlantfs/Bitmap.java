package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class Bitmap {

    private final ByteBuffer bitmap;

    Bitmap(int size) {
        this.bitmap = ByteBuffer.allocate(size);
    }

    static Bitmap create(SeekableByteChannel channel, long position, int blockSize) throws IOException {
        Bitmap result = new Bitmap(blockSize);
        channel.position(position);
        int written = channel.write(result.bitmap);
        assert written == result.bitmap.capacity();
        return result;
    }

    static Bitmap read(SeekableByteChannel channel, long position, int blockSize) throws IOException {
        Bitmap result = new Bitmap(blockSize);
        channel.position(position);
        int read = channel.read(result.bitmap);
        assert read == result.bitmap.capacity();
        return result;
    }

}

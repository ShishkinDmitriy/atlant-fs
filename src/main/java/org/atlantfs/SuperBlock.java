package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystemException;

final class SuperBlock {

    public static final int LENGTH = 1024;
    public static final short MAGIC = (short) 0xEF54;

    private int blockSize;

    static SuperBlock read(SeekableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        int read = channel.read(buffer);
        assert read == LENGTH;
        short magic = buffer.getShort();
        if (magic != MAGIC) {
            throw new FileSystemException("Bad magic for Atlant FS [" + magic + "]");
        }
        SuperBlock result = new SuperBlock();
        result.blockSize = buffer.getInt();
        return result;
    }

    static SuperBlock create(SeekableByteChannel channel, int blockSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        buffer.putShort(MAGIC);
        buffer.putInt(blockSize);
        int written = channel.write(buffer);
        assert written == LENGTH;
        SuperBlock result = new SuperBlock();
        result.blockSize = blockSize;
        return result;
    }

    int getBlockSize() {
        return blockSize;
    }

    int getInodeBitmapFirstBlock() {
        return 2;
    }

}

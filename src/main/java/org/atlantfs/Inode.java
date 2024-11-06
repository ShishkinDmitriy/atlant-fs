package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class Inode {

    private static final Logger log = Logger.getLogger(Inode.class.getName());
    static final int LENGTH = 128;

    /**
     * Size in bytes.
     */
    private long size;

    /**
     * Blocks count.
     */
    private long blocksCount;

    private IBlock iBlock;

    private volatile boolean dirty;

    /**
     * If an inode is locked, it may not be flushed from the cache (locked
     * counts the number of threads that have locked the inode)
     */
    private volatile int locked;

    private AtlantFileSystem fs;

    record Id(int value) {

        static final int LENGTH = 4;

        /**
         * Inode 0 is used for a null value, which means that there is no inode.
         */
        public static final Id NULL = new Id(0);

    }

//    static Bitmap create(SeekableByteChannel channel, long position, int blockSize) throws IOException {
//        log.finer(() -> "Creating inode [position=" + position + ", size=" + blockSize + "]...");
//        Bitmap result = new Bitmap(blockSize);
//        channel.position(position);
//        int written = channel.write(result.bitmap);
//        assert written == result.bitmap.capacity();
//        return result;
//    }

    static Inode read(ByteBuffer buffer) {
        buffer.position(buffer.position() + LENGTH);
        return new Inode();
    }

}

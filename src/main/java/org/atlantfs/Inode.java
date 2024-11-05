package org.atlantfs;

import java.util.logging.Logger;

class Inode {

    private static final Logger log = Logger.getLogger(Inode.class.getName());
    public static final int EXT2_GOOD_OLD_INODE_SIZE = 128;

    /**
     * the data constituting the inode itself
     */
    private byte[] data;

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
//
//    static Bitmap read(SeekableByteChannel channel, long position, int blockSize) throws IOException {
//        Bitmap result = new Bitmap(blockSize);
//        channel.position(position);
//        int read = channel.read(result.bitmap);
//        assert read == result.bitmap.capacity();
//        return result;
//    }


}

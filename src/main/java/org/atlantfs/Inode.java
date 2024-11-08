package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class Inode {

    private static final Logger log = Logger.getLogger(Inode.class.getName());
    static final int LENGTH = 128;

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

    int getLength(){
        return fs.getSuperBlock().getInodeSize();
    }

    record Id(int value) implements AbstractId {

        static final int LENGTH = 4;

        /**
         * Inode 0 is used for a null value, which means that there is no inode.
         */
        public static final Id NULL = new Id(0);

        /**
         * Inode 1 is used for root directory.
         */
        public static final Id ROOT = new Id(1);

        static Id of(int value) {
            return new Id(value);
        }

    }

    record Range(Inode.Id from, int length) implements AbstractRange<Id> {

        static Inode.Range of(Inode.Id from, int length) {
            assert from.value >= 0;
            assert length > 0;
            return new Inode.Range(from, length);
        }

        @Override
        public String toString() {
            return "Inode.Range{" +
                    "from=" + from +
                    ", length=" + length +
                    '}';
        }

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

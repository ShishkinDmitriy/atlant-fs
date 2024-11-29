package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

abstract class Inode<B extends Iblock> {

    private static final Logger log = Logger.getLogger(Inode.class.getName());

    static final int MIN_LENGTH = 8 + 4 + IblockType.LENGTH + 3;

    protected final AtlantFileSystem fileSystem;

    protected final transient Id id;

    protected B iblock;

    protected boolean dirty;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected Inode(AtlantFileSystem fileSystem, Id id, B iblock) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.iblock = iblock;
        checkInvariant();
    }

    static Inode<?> read(AtlantFileSystem fileSystem, ByteBuffer buffer, Id id) {
        assert buffer.remaining() == fileSystem.inodeSize() : "Read expects [inodeSize=" + fileSystem.inodeSize() + "] bytes, but actual [remaining=" + buffer.remaining() + "]";
        var size = buffer.getLong();
        var blocksCount = buffer.getInt();
        var iBlockType = IblockType.read(buffer);
        buffer.get(); // padding
        buffer.get();
        buffer.get();
        var iBlock = iBlockType.create(fileSystem, buffer, size, blocksCount);
        Inode<?> result;
        if (iBlock instanceof FileIblock fileIblock) {
            result = new FileInode(fileSystem, id, fileIblock);
        } else if (iBlock instanceof DirIblock dirIblock) {
            result = new DirInode(fileSystem, id, dirIblock);
        } else {
            throw new IllegalStateException("Should be one of file or dir");
        }
        assert !buffer.hasRemaining() : "Read should consume all bytes, but actual [remaining=" + buffer.remaining() + "]";
        return result;
    }

    protected void flush() {
        fileSystem.writeInode(this); // Can grow, TODO: add isDirty check
    }

    void flush(ByteBuffer buffer) {
        assert buffer.remaining() == inodeSize();
        buffer.putLong(iblock.size());
        buffer.putInt(iblock.blocksCount());
        iblock.type().write(buffer);
        buffer.put((byte) 0); // padding
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        iblock.flush(buffer);
        assert !buffer.hasRemaining();
    }

    void delete() throws IOException {
        try {
            beginWrite();
            iblock.delete();
        } finally {
            endWrite();
        }
    }

    void beginRead() {
        lock.readLock().lock();
    }

    void endRead() {
        lock.readLock().unlock();
    }

    void beginWrite() {
        lock.writeLock().lock();
    }

    void endWrite() {
        lock.writeLock().unlock();
    }

    protected void checkInvariant() {
        assert iblock != null : "Iblock should be specified";
    }

    int blockSize() {
        return fileSystem.blockSize();
    }

    int inodeSize() {
        return fileSystem.inodeSize();
    }

    public long size() {
        return iblock.size();
    }

    public AtlantFileSystem getFileSystem() {
        return fileSystem;
    }

    public FileType getFileType() {
        return iblock.type().fileType;
    }

    public Id getId() {
        return id;
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

        Id minus(Id val) {
            return new Id(value - val.value);
        }

        Id minus(int val) {
            return new Id(value - val);
        }

        @Override
        public String toString() {
            return "Inode.Id{" +
                    "value=" + value +
                    '}';
        }
    }

    record Range(Id from, int length) implements AbstractRange<Id> {

        static Range of(Id from, int length) {
            assert from.value >= 0;
            assert length > 0;
            return new Range(from, length);
        }

        @Override
        public String toString() {
            return "Inode.Range{" +
                    "from=" + from +
                    ", length=" + length +
                    '}';
        }

    }

}

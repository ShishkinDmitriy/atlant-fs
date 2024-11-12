package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

class Inode implements FileOperations, DirectoryOperations {

    private static final Logger log = Logger.getLogger(Inode.class.getName());

    private static final int MIN_LENGTH = 8 + 4 + IBlockType.LENGTH + 3;

    protected final AtlantFileSystem fileSystem;

    protected final transient Inode.Id id;

    protected long size;

    /**
     * Blocks count.
     */
    protected int blocksCount;

    protected IBlockType iBlockType;

    protected IBlock iBlock;

    protected boolean dirty;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Inode(AtlantFileSystem fileSystem, Id id, long size, int blocksCount, IBlockType iBlockType, IBlock iBlock) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.blocksCount = blocksCount;
        this.size = size;
        this.iBlockType = iBlockType;
        this.iBlock = iBlock;
        checkInvariant();
    }

    static Inode createRegularFile(AtlantFileSystem fileSystem, Inode.Id id) {
        return new Inode(fileSystem, id, 0, 0, IBlockType.INLINE_DATA, new Data(new byte[0], iBlockLength(fileSystem)));
    }

    static Inode createDirectory(AtlantFileSystem fileSystem, Inode.Id id) {
        return new Inode(fileSystem, id, 0, 0, IBlockType.INLINE_DIR_LIST, new DirEntryList(iBlockLength(fileSystem)));
    }

    static Inode read(AtlantFileSystem fileSystem, ByteBuffer buffer, Inode.Id id) {
        assert buffer.remaining() == fileSystem.inodeSize() : "Read expects [inodeSize=" + fileSystem.inodeSize() + "] bytes, but actual [remaining=" + buffer.remaining() + "]";
        var size = buffer.getLong();
        var blocksCount = buffer.getInt();
        var iBlockType = IBlockType.read(buffer);
        buffer.get(); // padding
        buffer.get();
        buffer.get();
        var iBlock = iBlockType.create(fileSystem, buffer);
        assert !buffer.hasRemaining() : "Read should consume all bytes, but actual [remaining=" + buffer.remaining() + "]";
        return new Inode(fileSystem, id, size, blocksCount, iBlockType, iBlock);
    }

    void write(ByteBuffer buffer) {
        assert buffer.remaining() == fileSystem.inodeSize();
        buffer.putLong(size);
        buffer.putInt(blocksCount);
        iBlockType.write(buffer);
        buffer.put((byte) 0); // padding
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        iBlock.write(buffer);
        assert !buffer.hasRemaining();
    }

    @Override
    public Iterator<DirEntry> iterator() {
        try {
            beginRead();
            return directoryOperations().iterator();
        } finally {
            endRead();
        }
    }

    @Override
    public DirEntry add(Id inode, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        try {
            beginWrite();
            var result = directoryOperations().add(inode, fileType, name);
            fileSystem.writeInode(id, this::write);
            return result;
        } catch (DirEntryListOfMemoryException e) {
            return upgradeAndAdd(directoryOperations -> {
                try {
                    return directoryOperations.add(inode, fileType, name);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        } finally {
            endWrite();
        }
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        try {
            beginRead();
            return directoryOperations().get(name);
        } finally {
            endRead();
        }
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        try {
            beginWrite();
            directoryOperations().rename(name, newName);
            fileSystem.writeInode(id, this::write); // Can grow, TODO: add isDirty check
        } catch (DirEntryListOfMemoryException e) {
            upgradeAndAdd(directoryOperations -> {
                try {
                    directoryOperations.rename(name, newName);
                    return null;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        } finally {
            endWrite();
        }
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        try {
            beginWrite();
            directoryOperations().delete(name);
            fileSystem.writeInode(id, this::write);
        } finally {
            endWrite();
        }
    }

    @Override
    public int write(long position, ByteBuffer buffer) {
        try {
            beginWrite();
            return fileOperations().write(position, buffer);
        } finally {
            endWrite();
        }
    }

    @Override
    public int read(long position, ByteBuffer buffer) {
        try {
            beginRead();
            return fileOperations().read(position, buffer);
        } finally {
            endRead();
        }
    }

    private <T> T upgradeAndAdd(Function<DirectoryOperations, T> func) throws BitmapRegionOutOfMemoryException {
        assert iBlockType == IBlockType.INLINE_DIR_LIST;
        var blockSize = blockSize();
        var dirEntryList = (DirEntryList) iBlock;
        dirEntryList.resize(blockSize);
        var dirEntry = func.apply(dirEntryList);
        var reserved = reserveBlock();
        size = (long) blocksCount * blockSize;
        fileSystem.writeBlock(reserved, dirEntryList::write);
        var blockList = new BlockMapping(this);
        blockList.getAddresses().add(reserved);
        iBlock = blockList;
        iBlockType = IBlockType.DIR_LIST;
        dirty = true;
        checkInvariant();
        return dirEntry;
    }

    protected Block.Id reserveBlock() throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlock();
        blocksCount++;
        return reserved;
    }

    private DirectoryOperations directoryOperations() {
        ensureDirectory();
        return (DirectoryOperations) iBlock;
    }

    private FileOperations fileOperations() {
        ensureRegularFile();
        return (FileOperations) iBlock;
    }

    private void beginRead() {
        readLock().lock();
    }

    private void endRead() {
        readLock().unlock();
    }

    private void beginWrite() {
        writeLock().lock();
    }

    private void endWrite() {
        writeLock().unlock();
    }

    void writeBlock(Block.Id blockId, Consumer<ByteBuffer> consumer) {
        fileSystem.writeBlock(blockId, consumer);
    }

    ByteBuffer readBlock(Block.Id blockId) {
        return fileSystem.readBlock(blockId);
    }

    private void checkInvariant() {
        assert size <= (long) blocksCount * blockSize();
        assert iBlockType != null : "IBlock type should be specified";
    }

    private void ensureRegularFile() {
        if (iBlockType.getFileType() != FileType.REGULAR_FILE) {
            throw new IllegalStateException("Not a file");
        }
    }

    private void ensureDirectory() {
        if (iBlockType.getFileType() != FileType.DIRECTORY) {
            throw new IllegalStateException("Not a directory");
        }
    }

    int blockSize() {
        return fileSystem.blockSize();
    }

    static int iBlockLength(AtlantFileSystem fileSystem) {
        return fileSystem.inodeSize() - MIN_LENGTH;
    }

    public long getSize() {
        return size;
    }

    public AtlantFileSystem getFileSystem() {
        return fileSystem;
    }

    public int getBlocksCount() {
        return blocksCount;
    }

    public ReentrantReadWriteLock.ReadLock readLock() {
        return lock.readLock();
    }

    public ReentrantReadWriteLock.WriteLock writeLock() {
        return lock.writeLock();
    }

    public FileType getFileType() {
        return iBlockType.fileType;
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

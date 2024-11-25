package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Logger;

class Inode implements FileOperations, DirectoryOperations {

    private static final Logger log = Logger.getLogger(Inode.class.getName());

    private static final int MIN_LENGTH = 8 + 4 + IBlockType.LENGTH + 3;

    private final AtlantFileSystem fileSystem;

    private final transient Inode.Id id;

    private long size;

    /**
     * Blocks count.
     */
    private int blocksCount;

    private IBlockType iBlockType;

    private IBlock iBlock;

    private boolean dirty;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Inode(AtlantFileSystem fileSystem, Id id, long size, int blocksCount) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.size = size;
        this.blocksCount = blocksCount;
        this.iBlockType = IBlockType.FILE_INLINE_DATA;
        this.iBlock = new Data(new byte[iBlockLength(fileSystem.inodeSize())], (int) size);
        checkInvariant();
    }

    private Inode(AtlantFileSystem fileSystem, Id id, long size, int blocksCount, IBlockType iBlockType, IBlock iBlock) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.blocksCount = blocksCount;
        this.size = size;
        this.iBlockType = iBlockType;
        this.iBlock = iBlock;
        checkInvariant();
    }

    static Inode createRegularFile(AtlantFileSystem fileSystem, Inode.Id id) {
        var inodeSize = fileSystem.inodeSize();
        var iBlockLength = iBlockLength(inodeSize);
        return new Inode(fileSystem, id, 0, 0, IBlockType.FILE_INLINE_DATA, new Data(new byte[iBlockLength], 0));
    }

    static Inode createDirectory(AtlantFileSystem fileSystem, Inode.Id id) {
        var inodeSize = fileSystem.inodeSize();
        var iBlockLength = iBlockLength(inodeSize);
        return new Inode(fileSystem, id, 0, 0, IBlockType.DIR_INLINE_LIST, new DirEntryList(iBlockLength));
    }

    static Inode read(AtlantFileSystem fileSystem, ByteBuffer buffer, Inode.Id id) {
        assert buffer.remaining() == fileSystem.inodeSize() : "Read expects [inodeSize=" + fileSystem.inodeSize() + "] bytes, but actual [remaining=" + buffer.remaining() + "]";
        var size = buffer.getLong();
        var blocksCount = buffer.getInt();
        var iBlockType = IBlockType.read(buffer);
        buffer.get(); // padding
        buffer.get();
        buffer.get();
        var inode = new Inode(fileSystem, id, size, blocksCount);
        var iBlock = iBlockType.create(inode, buffer);
        inode.iBlockType = iBlockType;
        inode.iBlock = iBlock;
        inode.checkInvariant();
        assert !buffer.hasRemaining() : "Read should consume all bytes, but actual [remaining=" + buffer.remaining() + "]";
        return inode;
    }

    void write(ByteBuffer buffer) {
        assert buffer.remaining() == inodeSize();
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
    public DirEntry add(Id inode, FileType fileType, String name) throws AbstractOutOfMemoryException {
        try {
            beginWrite();
            var result = directoryOperations().add(inode, fileType, name);
            flush();
            return result;
        } catch (DirEntryListOfMemoryException e) {
            upgradeInlineDirList();
            var result = directoryOperations().add(inode, fileType, name);
            flush();
            return result;
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
    public void rename(String name, String newName) throws NoSuchFileException, AbstractOutOfMemoryException {
        try {
            beginWrite();
            directoryOperations().rename(name, newName);
            flush();
        } catch (DirEntryListOfMemoryException e) {
            upgradeInlineDirList();
            directoryOperations().rename(name, newName);
            flush();
        } finally {
            endWrite();
        }
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        try {
            beginWrite();
            directoryOperations().delete(name);
            flush();
        } finally {
            endWrite();
        }
    }

    @Override
    public void delete() throws IOException {
        try {
            beginWrite();
            if (isDirectory()) {
                ((DirectoryOperations) iBlock).delete();
            } else if (isRegularFile()) {
                ((FileOperations) iBlock).delete();
            }
//            fileSystem.freeInode(id);
        } finally {
            endWrite();
        }
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DataOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var initial = buffer.position();
        try {
            beginWrite();
            var written = fileOperations().write(position, buffer);
            size = Math.max(size, position + initial + written);
            flush();
            return written;
        } catch (DataOutOfMemoryException e) {
            upgradeInlineData();
            var written = fileOperations().write(position, buffer);
            size = Math.max(size, position + initial + written);
            flush();
            return written;
        } finally {
            endWrite();
        }
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        try {
            beginRead();
            var read = fileOperations().read(position, buffer);
            buffer.flip();
            return read;
        } finally {
            endRead();
        }
    }

    private void upgradeInlineDirList() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline dir list to block mapping...");
        assert iBlockType == IBlockType.DIR_INLINE_LIST : "Only DIR_INLINE_LIST can be upgraded";
        assert blocksCount == 0 : "Should be no blocks before upgrade";
        var dirEntryList = (DirEntryList) iBlock;
//        var blockSize = blockSize();
//        dirEntryList.resize(blockSize);
//        var reserved = reserveBlock();
//        writeBlock(reserved, dirEntryList::write);
        iBlock = DirBlockMapping.init(this, dirEntryList);
        iBlockType = IBlockType.DIR_BLOCK_MAPPING;
        size = (long) blocksCount * blockSize();
        dirty = true;
        assert blocksCount == 0 || blocksCount == 1 : "Can be zero if upgraded from empty data or 1 if had content before";
        checkInvariant();
    }

    private void upgradeInlineData() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline data to block mapping...");
        assert iBlockType == IBlockType.FILE_INLINE_DATA : "Only FILE_INLINE_DATA can be upgraded";
        assert size >= 0 : "Size can be 0 if were no content in file or positive if there is small amount";
        assert blocksCount == 0 : "Should be no blocks before upgrade";
        var data = (Data) iBlock;
        iBlock = FileBlockMapping.init(this, data);
        iBlockType = IBlockType.FILE_BLOCK_MAPPING;
        dirty = true;
        assert blocksCount == 0 || blocksCount == 1 : "Can be zero if upgraded from empty data or 1 if had content before";
        checkInvariant();
    }

    Block.Id reserveBlock() throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlock();
        blocksCount++;
        return reserved;
    }

    /**
     * Reserve number of blocks, but only 1 for data, so blocksCount will be increased only on 1.
     *
     * @param size the number of blocks to reserve
     * @return the list of block ranges reserved
     * @throws BitmapRegionOutOfMemoryException if no more free blocks
     */
    protected List<Block.Range> reserveBlocks(int size) throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlocks(size);
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

    private void flush() {
        fileSystem.writeInode(this); // Can grow, TODO: add isDirty check
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

    void writeBlock(Block.Id blockId, Consumer<ByteBuffer> consumer) {
        writeBlock(blockId, 0, consumer);
    }

    int writeBlock(Block.Id blockId, int offset, Consumer<ByteBuffer> consumer) {
        return fileSystem.writeBlock(blockId, offset, consumer);
    }

    ByteBuffer readBlock(Block.Id blockId) {
        return fileSystem.readBlock(blockId);
    }

    private void checkInvariant() {
//        assert size <= (long) blocksCount * blockSize();
        assert iBlockType != null : "IBlock type should be specified";
    }

    boolean isDirectory() {
        return iBlockType.fileType == FileType.DIRECTORY;
    }

    boolean isRegularFile() {
        return iBlockType.fileType == FileType.REGULAR_FILE;
    }

    void ensureRegularFile() {
        if (!isRegularFile()) {
            throw new IllegalStateException("Not a file");
        }
    }

    void ensureDirectory() {
        if (!isDirectory()) {
            throw new IllegalStateException("Not a directory");
        }
    }

    int blockSize() {
        return fileSystem.blockSize();
    }

    int inodeSize() {
        return fileSystem.inodeSize();
    }

    static int iBlockLength(int inodeSize) {
        return inodeSize - MIN_LENGTH;
    }

    public long getSize() {
        return size;
    }

    public AtlantFileSystem getFileSystem() {
        return fileSystem;
    }

    public int blocksCount() {
        return blocksCount;
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

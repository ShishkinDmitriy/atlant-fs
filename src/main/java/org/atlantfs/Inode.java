package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

class Inode implements FileOperations, DirectoryOperations {

    private static final Logger log = Logger.getLogger(Inode.class.getName());

    static final int MIN_LENGTH = 8 + 4 + IBlockType.LENGTH + 3;

    private final AtlantFileSystem fileSystem;

    private final transient Inode.Id id;

    private IBlock iBlock;

    private boolean dirty;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Inode(AtlantFileSystem fileSystem, Id id, IBlock iBlock) {
        this.fileSystem = fileSystem;
        this.id = id;
        this.iBlock = iBlock;
        checkInvariant();
    }

    static Inode createRegularFile(AtlantFileSystem fileSystem, Inode.Id id) {
        return new Inode(fileSystem, id, DataIblock.init(fileSystem));
    }

    static Inode createDirectory(AtlantFileSystem fileSystem, Inode.Id id) {
        return new Inode(fileSystem, id, DirEntryListIblock.init(fileSystem));
    }

    static Inode read(AtlantFileSystem fileSystem, ByteBuffer buffer, Inode.Id id) {
        assert buffer.remaining() == fileSystem.inodeSize() : "Read expects [inodeSize=" + fileSystem.inodeSize() + "] bytes, but actual [remaining=" + buffer.remaining() + "]";
        var size = buffer.getLong();
        var blocksCount = buffer.getInt();
        var iBlockType = IBlockType.read(buffer);
        buffer.get(); // padding
        buffer.get();
        buffer.get();
        var iBlock = iBlockType.create(fileSystem, buffer, size, blocksCount);
        var inode = new Inode(fileSystem, id, iBlock);
        assert !buffer.hasRemaining() : "Read should consume all bytes, but actual [remaining=" + buffer.remaining() + "]";
        return inode;
    }

    void flush(ByteBuffer buffer) {
        assert buffer.remaining() == inodeSize();
        buffer.putLong(iBlock.size());
        buffer.putInt(iBlock.blocksCount());
        iBlock.type().write(buffer);
        buffer.put((byte) 0); // padding
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        iBlock.flush(buffer);
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
        try {
            beginWrite();
            var written = fileOperations().write(position, buffer);
            flush();
            return written;
        } catch (DataOutOfMemoryException e) {
            upgradeInlineData();
            var written = fileOperations().write(position, buffer);
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
        assert iBlock instanceof DirEntryListIblock : "Only DIR_INLINE_LIST can be upgraded";
        var dirEntryList = (DirEntryListIblock) iBlock;
        iBlock = DirBlockMapping.init(fileSystem, dirEntryList.entryList());
        dirty = true;
        checkInvariant();
    }

    private void upgradeInlineData() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline data to block mapping...");
        assert iBlock instanceof DataIblock : "Only FILE_INLINE_DATA can be upgraded";
        var data = (DataIblock) iBlock;
        iBlock = FileBlockMapping.init(fileSystem, data.data());
        dirty = true;
        checkInvariant();
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

    private void checkInvariant() {
        assert iBlock != null : "Iblock should be specified";
    }

    boolean isDirectory() {
        return iBlock.type().fileType == FileType.DIRECTORY;
    }

    boolean isRegularFile() {
        return iBlock.type().fileType == FileType.REGULAR_FILE;
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

    public long size() {
        return iBlock.size();
    }

    public AtlantFileSystem getFileSystem() {
        return fileSystem;
    }

    public FileType getFileType() {
        return iBlock.type().fileType;
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

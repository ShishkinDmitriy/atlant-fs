package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

public class AtlantFileSystem extends FileSystem {

    private static final Logger log = Logger.getLogger(AtlantFileSystem.class.getName());

    private static final ThreadLocal<ByteBuffer> blockByteBuffer = new ThreadLocal<>();
    private static final ThreadLocal<ByteBuffer> inodeByteBuffer = new ThreadLocal<>();

    private final AtlantFileSystemProvider provider;
    private final AtlantStatistics statistics = new AtlantStatistics();
    private final Path atlant;
    private final SuperBlock superBlock;
    private final DataBitmapRegion dataBitmapRegion = new DataBitmapRegion(this);
    private final InodeBitmapRegion inodeBitmapRegion = new InodeBitmapRegion(this);
    private final InodeTableRegion inodeTableRegion;
    private volatile boolean isOpen = true;

    public AtlantFileSystem(AtlantFileSystemProvider provider, Path atlant, Map<String, ?> env) throws IOException {
        assert AtlantFileChannel.notExists();
        this.provider = provider;
        this.atlant = atlant;
        if (Files.exists(atlant)) {
            log.finer(() -> "Opening Atlant file system [path=" + atlant.toAbsolutePath() + "]...");
            try (var _ = AtlantFileChannel.openForRead(atlant)) {
                var channel = AtlantFileChannel.get();
                var buffer = ByteBuffer.allocate(SuperBlock.LENGTH);
                var read = channel.read(buffer);
                statistics.incrementReadCalls();
                statistics.addReadBytes(read);
                buffer.flip();
                superBlock = SuperBlock.read(this, buffer);
                inodeTableRegion = InodeTableRegion.read(this);
                log.fine(() -> "Successfully opened new Atlant file system [path=" + atlant.toAbsolutePath() + "]");
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to open Atlant file system [path=" + atlant.toAbsolutePath() + "]", e);
                throw e;
            }
        } else {
            log.finer(() -> "Creating new Atlant file system [path=" + atlant.toAbsolutePath() + "]...");
            superBlock = SuperBlock.init(this, AtlantConfig.fromMap(env));
            try (var _ = AtlantFileChannel.openForCreate(atlant)) {
                superBlock.flush();
                dataBitmapRegion.init();
                inodeBitmapRegion.init();
                inodeTableRegion = new InodeTableRegion(this);
                log.fine(() -> "Successfully created new Atlant file system [path=" + atlant.toAbsolutePath() + "]");
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to create Atlant file system [path=" + atlant.toAbsolutePath() + "]", e);
                throw e;
            }
        }
    }

    void createDirectory(AtlantPath dir) throws IOException {
        try (var _ = AtlantFileChannel.openForWrite(atlant)) {
            var _ = locateDir(dir, CREATE_NEW);
        }
    }

    public DirectoryStream<Path> newDirectoryStream(AtlantPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var atlant = AtlantFileChannel.openForWrite(this.atlant);
        try {
            var dirInode = locateDir(dir);
            try {
                dirInode.beginWrite();
                var iterator = dirInode.iterator();
                return new DirectoryStream<>() {
                    @Override
                    public Iterator<Path> iterator() {
                        return new Iterator<>() {

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public Path next() {
                                return getPath(dir.toString(), iterator.next().getName());
                            }

                        };
                    }

                    @Override
                    public void close() throws IOException {
                        atlant.close();
                        dirInode.endWrite();
                    }
                };
            } catch (Exception | AssertionError e) {
                dirInode.endWrite();
                throw e;
            }
        } catch (IOException e) {
            atlant.close();
            throw e;
        }
    }

    private DirInode locateDir(AtlantPath path) throws NoSuchFileException, FileAlreadyExistsException, NotEnoughSpaceException {
        return locateDir(path, Set.of());
    }

    private DirInode locateDir(AtlantPath path, OpenOption... options) throws NoSuchFileException, FileAlreadyExistsException, NotEnoughSpaceException {
        return locateDir(path, new HashSet<>(Arrays.asList(options)));
    }

    private DirInode locateDir(AtlantPath path, Set<? extends OpenOption> options) throws NoSuchFileException, FileAlreadyExistsException, NotEnoughSpaceException {
        if (path.isRoot()) {
            return root();
        }
        var parentInode = locateDir(path.getParent(), options);
        var fileName = path.getFileName().toString();
        try {
            var dirEntry = parentInode.get(fileName);
            return findDirInode(dirEntry.getInode());
        } catch (NoSuchFileException e) {
            if (!options.contains(CREATE) && !options.contains(CREATE_NEW)) {
                throw new NoSuchFileException(path.toString());
            }
            var newInode = inodeTableRegion.createDirectory();
            var _ = parentInode.addDir(newInode.getId(), fileName);
            return newInode;
        }
    }

    private FileInode locateFile(AtlantPath path, Set<? extends OpenOption> options) throws NoSuchFileException, FileAlreadyExistsException, NotEnoughSpaceException {
        var parentInode = locateDir(path.getParent(), options);
        var fileName = path.getFileName().toString();
        try {
            var dirEntry = parentInode.get(fileName);
            if (options.contains(CREATE_NEW)) {
                throw new FileAlreadyExistsException(path.toString());
            }
            return findFileInode(dirEntry.getInode());
        } catch (NoSuchFileException e) {
            if (!options.contains(CREATE) && !options.contains(CREATE_NEW)) {
                throw new NoSuchFileException(path.toString());
            }
            var fileInode = inodeTableRegion.createFile();
            var _ = parentInode.addFile(fileInode.getId(), fileName);
            return fileInode;
        }
    }

    private Inode<?> locateAny(AtlantPath path) throws NoSuchFileException, FileAlreadyExistsException, NotEnoughSpaceException {
        if (path.isRoot()) {
            return root();
        }
        var parentInode = locateDir(path.getParent());
        var fileName = path.getFileName().toString();
        var dirEntry = parentInode.get(fileName);
        return inodeTableRegion.get(dirEntry.getInode());
    }

    public SeekableByteChannel newByteChannel(AtlantPath absolutePath, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {
        var atlant = options.contains(WRITE) ? AtlantFileChannel.openForWrite(this.atlant) : AtlantFileChannel.openForRead(this.atlant);
        FileInode fileInode = null;
        try {
            fileInode = locateFile(absolutePath, options);
            if (options.contains(WRITE) || options.contains(APPEND)) {
                fileInode.beginWrite();
            } else {
                fileInode.beginRead();
            }
            FileInode finalInode = fileInode;
            return new SeekableByteChannel() {

                private long position;
                private boolean open = true;

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    checkOpen();
                    var read = finalInode.read(position, dst);
                    position += read;
                    return read;
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    checkOpen();
                    return finalInode.write(position, src);
                }

                @Override
                public long position() {
                    return position;
                }

                @Override
                public SeekableByteChannel position(long newPosition) throws IOException {
                    checkOpen();
                    if (newPosition < 0) {
                        throw new IllegalArgumentException();
                    }
                    this.position = newPosition;
                    return this;
                }

                @Override
                public long size() {
                    return finalInode.size();
                }

                @Override
                public SeekableByteChannel truncate(long size) {
                    return this;
                }

                @Override
                public boolean isOpen() {
                    return open;
                }

                @Override
                public void close() throws IOException {
                    if (!open) {
                        return;
                    }
                    open = false;
                    if (options.contains(WRITE) || options.contains(APPEND)) {
                        finalInode.endWrite();
                    } else {
                        finalInode.endRead();
                    }
                    atlant.close();
                }

                private void checkOpen() throws ClosedChannelException {
                    if (!isOpen()) {
                        throw new ClosedChannelException();
                    }
                }

            };
        } catch (IOException | AssertionError e) {
            atlant.close();
            if (fileInode != null) {
                if (options.contains(WRITE) || options.contains(APPEND)) {
                    fileInode.endWrite();
                } else {
                    fileInode.endRead();
                }
            }
            throw e;
        }
    }

    void delete(AtlantPath absolutePath) throws IOException {
        try (var _ = AtlantFileChannel.openForWrite(atlant)) {
            DirInode parent = locateDir(absolutePath.getParent());
            try {
                parent.beginWrite();
                var fileName = absolutePath.getFileName().toString();
                var dirEntry = parent.get(fileName);
                Inode<?> inode = inodeTableRegion.get(dirEntry.getInode());
                inode.delete();
                inodeTableRegion.delete(inode.getId());
                parent.remove(fileName);
            } finally {
                parent.endWrite();
            }
        }
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() {
        if (!isOpen) {
            return;
        }
        log.finer(() -> "Closing Atlant file system [path=" + atlant.toAbsolutePath() + "]...");
        assert AtlantFileChannel.notExists(); // TODO: Can be closed async?
        blockByteBuffer.remove();
        inodeByteBuffer.remove();
        isOpen = false;
        provider.removeFileSystem(atlant);
        log.fine(() -> "Successfully closed Atlant file system [path=" + atlant.toAbsolutePath() + "]");
        statistics.print();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return List.of(new AtlantPath(this, "/".getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return null;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of();
    }

    @Override
    public Path getPath(String first, String... more) {
        if (more == null || more.length == 0) {
            return new AtlantPath(this, first.getBytes(StandardCharsets.UTF_8));
        }
        if (first.endsWith("/")) {
            first = first.substring(0, first.length() - 1);
        }
        return new AtlantPath(this, (first + getSeparator() + String.join(getSeparator(), more)).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        return null;
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        return null;
    }

    @Override
    public WatchService newWatchService() {
        return null;
    }

    ByteBuffer getBlockByteBuffer() {
        return getBuffer(blockByteBuffer, blockSize());
    }

    ByteBuffer getInodeByteBuffer() {
        return getBuffer(inodeByteBuffer, inodeSize());
    }

    private ByteBuffer getBuffer(ThreadLocal<ByteBuffer> threadLocal, int capacity) {
        var buffer = threadLocal.get();
        if (buffer != null) {
            buffer.clear();
            return buffer;
        }
        buffer = ByteBuffer.allocateDirect(capacity);
        threadLocal.set(buffer);
        return buffer;
    }

    int blockSize() {
        return superBlock.blockSize();
    }

    int inodeSize() {
        return superBlock.inodeSize();
    }

    int iblockSize() {
        return inodeSize() - Inode.MIN_LENGTH;
    }

    DirInode root() {
        return inodeTableRegion.root();
    }

    DirInode findDirInode(Inode.Id inodeId) throws FileAlreadyExistsException {
        var inode = inodeTableRegion.get(inodeId);
        if (inode instanceof DirInode dirInode) {
            return dirInode;
        }
        throw new FileAlreadyExistsException("File of type [" + inode.getFileType() + "] already exists, expected [" + FileType.DIRECTORY + "]");
    }

    FileInode findFileInode(Inode.Id inodeId) throws FileAlreadyExistsException {
        var inode = inodeTableRegion.get(inodeId);
        if (inode instanceof FileInode fileInode) {
            return fileInode;
        }
        throw new FileAlreadyExistsException("File of type [" + inode.getFileType() + "] already exists, expected [" + FileType.REGULAR_FILE + "]");
    }

    Block.Id reserveBlock() throws BitmapRegion.NotEnoughSpaceException {
        log.finer(() -> "Reserving 1 block...");
        var reserved = dataBitmapRegion.reserve();
        log.fine(() -> "Successfully reserved 1 block [" + reserved + "]");
        return reserved;
    }

    List<Block.Range> reserveBlocks(int size) throws BitmapRegion.NotEnoughSpaceException {
        log.finer(() -> "Reserving [" + size + "] blocks...");
        var reserved = dataBitmapRegion.reserve(size);
        log.fine(() -> "Successfully reserved [" + size + "] block [" + reserved + "]");
        return reserved;
    }

    Inode.Id reserveInode() throws BitmapRegion.NotEnoughSpaceException {
        log.finer(() -> "Reserving 1 inode...");
        var reserved = inodeBitmapRegion.reserve();
        log.fine(() -> "Successfully reserved 1 inode [" + reserved + "]");
        return reserved;
    }

    void freeBlock(Block.Id inodeId) {
        dataBitmapRegion.free(inodeId);
    }

    void freeBlocks(List<Block.Id> inodeIds) {
        dataBitmapRegion.free(inodeIds);
    }

    void freeInode(Inode.Id inodeId) {
        inodeBitmapRegion.free(inodeId);
    }

    ByteBuffer readBlock(Block.Id blockId) {
        var buffer = getBlockByteBuffer();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            var blockPosition = blockPosition(blockId);
            channel.position(blockPosition);
            log.finer(() -> "Reading from Atlant file [blockId=" + blockId + " (" + blockPosition + "), position=" + blockPosition + "]...");
            var read = channel.read(buffer);
            statistics.incrementReadCalls();
            statistics.addReadBytes(read);
            var blockSize = blockSize();
            if (read < blockSize) {
                // if the channel has reached end-of-stream
                while (buffer.hasRemaining()) {
                    buffer.put((byte) 0);
                }
            }
            buffer.flip();
            assert buffer.position() == 0 : "Buffer should has zero position, but have [" + buffer.position() + "]";
            assert buffer.limit() == blockSize : "Buffer should has block size limit [" + blockSize + "], but have [" + buffer.limit() + "]";
            assert buffer.remaining() == blockSize : "Buffer should has block size remaining [" + blockSize + "], but have [" + buffer.remaining() + "]";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    Inode<?> readInode(Inode.Id inodeId) {
        var buffer = getInodeByteBuffer();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            var inodePosition = inodePosition(inodeId);
            channel.position(inodePosition);
            var read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("Unexpected EOF");
            }
            statistics.incrementReadCalls();
            statistics.addReadBytes(read);
            buffer.flip();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Inode.read(this, buffer, inodeId);
    }

    int writeBlock(Block.Id blockId, Consumer<ByteBuffer> consumer) {
        return writeBlock(blockId, 0, consumer);
    }

    int writeBlock(Block.Id blockId, int offset, Consumer<ByteBuffer> consumer) {
        assert offset >= 0;
        assert offset < blockSize();
        var buffer = getBlockByteBuffer();
        consumer.accept(buffer);
        buffer.flip();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            var blockPosition = blockPosition(blockId);
            var position = blockPosition + offset;
            log.finer(() -> "Writing into Atlant file [blockId=" + blockId + " (" + blockPosition + "), position=" + position + ", bytes=" + buffer.remaining() + "]...");
            channel.position(position);
            var written = channel.write(buffer);
            statistics.incrementWriteCalls();
            statistics.addWriteBytes(written);
            return written;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void writeInode(Inode<?> inode) {
        var buffer = getInodeByteBuffer();
        inode.flush(buffer);
        buffer.flip();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            var inodeId = inode.getId();
            var inodePosition = inodePosition(inodeId);
            log.finer(() -> "Writing into Atlant file [inodeId=" + inodeId + ", position=" + inodePosition + ", bytes=" + buffer.remaining() + "]...");
            channel.position(inodePosition);
            var written = channel.write(buffer);
            statistics.incrementWriteCalls();
            statistics.addWriteBytes(written);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long blockPosition(Block.Id blockId) {
        return (long) blockId.value() * blockSize();
    }

    private long inodePosition(Inode.Id inodeId) {
        return (long) superBlock.firstBlockOfInodeTables().value() * blockSize()
                + ((long) inodeSize() * inodeId.minus(1).value());
    }

    SuperBlock superBlock() {
        return superBlock;
    }

    public AtlantFileAttributes readAttributes(AtlantPath absolutePath, LinkOption[] options) throws IOException {
        try (var _ = AtlantFileChannel.openForRead(atlant)) {
            var inode = locateAny(absolutePath);
            // TODO: Add lock
            return AtlantFileAttributes.from(inode);
        }
    }

}

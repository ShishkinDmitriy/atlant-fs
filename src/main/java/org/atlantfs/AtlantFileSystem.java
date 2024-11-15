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
    private final Path path;
    private final SuperBlock superBlock;
    private final DataBitmapRegion dataBitmapRegion = new DataBitmapRegion(this);
    private final InodeBitmapRegion inodeBitmapRegion = new InodeBitmapRegion(this);
    private final InodeTableRegion inodeTableRegion;
    private volatile boolean isOpen = true;

    public AtlantFileSystem(AtlantFileSystemProvider provider, Path path, Map<String, ?> env) throws IOException {
        assert AtlantFileChannel.notExists();
        this.provider = provider;
        this.path = path;
        if (Files.exists(path)) {
            log.finer(() -> "Opening Atlant file system [path=" + path.toAbsolutePath() + "]...");
            try (var _ = AtlantFileChannel.openForRead(path)) {
                var channel = AtlantFileChannel.get();
                var buffer = ByteBuffer.allocate(SuperBlock.LENGTH);
                channel.read(buffer);
//                assert !buffer.hasRemaining();
                buffer.flip();
                this.superBlock = SuperBlock.read(buffer);
                this.inodeTableRegion = InodeTableRegion.read(this);
                log.fine(() -> "Successfully opened new Atlant file system [path=" + path.toAbsolutePath() + "]");
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to open Atlant file system [path=" + path.toAbsolutePath() + "]", e);
                throw e;
            }
        } else {
            log.finer(() -> "Creating new Atlant file system [path=" + path.toAbsolutePath() + "]...");
            var config = AtlantConfig.fromMap(env);
            this.superBlock = SuperBlock.init(config);
            try (var _ = AtlantFileChannel.openForCreate(path)) {
                writeBlock(Block.Id.ZERO, superBlock::write);
                dataBitmapRegion.init();
                inodeBitmapRegion.init();
                this.inodeTableRegion = new InodeTableRegion(this);
                log.fine(() -> "Successfully created new Atlant file system [path=" + path.toAbsolutePath() + "]");
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to create Atlant file system [path=" + path.toAbsolutePath() + "]", e);
                throw e;
            }
        }
    }

    void createDirectory(AtlantPath dir) throws IOException {
        try (var _ = AtlantFileChannel.openForWrite(path)) {
            locate(dir, FileType.DIRECTORY, CREATE_NEW);
        }
    }

    public DirectoryStream<Path> newDirectoryStream(AtlantPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var atlant = AtlantFileChannel.openForRead(path);
        Inode inode = null;
        try {
            inode = locate(dir, FileType.DIRECTORY);
            inode.beginRead();
            Inode finalInode = inode;
            var iterator = finalInode.iterator();
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
                    finalInode.endRead();
                }
            };
        } catch (IOException | AssertionError e) {
            atlant.close();
            if (inode != null) {
                inode.endRead();
            }
            throw e;
        }
    }

    private Inode locate(AtlantPath absolutePath, OpenOption... options) throws NoSuchFileException, FileAlreadyExistsException, BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException {
        return locate(absolutePath, null, new HashSet<>(Arrays.asList(options)));
    }

    private Inode locate(AtlantPath absolutePath, FileType fileType, OpenOption... options) throws NoSuchFileException, FileAlreadyExistsException, BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException {
        return locate(absolutePath, fileType, new HashSet<>(Arrays.asList(options)));
    }

    private Inode locate(AtlantPath absolutePath, FileType fileType, Set<? extends OpenOption> options) throws NoSuchFileException, FileAlreadyExistsException, BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException {
        if (absolutePath.equals(absolutePath.getRoot())) {
            return inodeTableRegion.root();
        }
        var inode = inodeTableRegion.root();
        for (Path path : absolutePath.getParent()) {
            var fileName = path.getFileName().toString();
            try {
                DirEntry dirEntry = inode.get(fileName);
                inode = findInode(dirEntry.getInode());
            } catch (NoSuchFileException e) {
                if (!options.contains(CREATE) && !options.contains(CREATE_NEW)) {
                    throw new NoSuchFileException(path.toString());
                }
                var newInode = inodeTableRegion.createDirectory();
                inode.addDirectory(newInode.getId(), fileName);
                inode = newInode;
            }
        }
        var fileName = absolutePath.getFileName().toString();
        try {
            DirEntry dirEntry = inode.get(fileName);
            if (options.contains(CREATE_NEW)) {
                throw new FileAlreadyExistsException(path.toString());
            }
            inode = findInode(dirEntry.getInode());
            if (fileType != null && inode.getFileType() != fileType) {
                throw new FileAlreadyExistsException("File of type [" + inode.getFileType() + "] already exists");
            }
            return inode;
        } catch (NoSuchFileException e) {
            if (!options.contains(CREATE) && !options.contains(CREATE_NEW)) {
                throw new NoSuchFileException(path.toString());
            }
            return switch (fileType) {
                case DIRECTORY -> {
                    var directory = inodeTableRegion.createDirectory();
                    inode.addDirectory(directory.getId(), fileName);
                    yield directory;
                }
                case REGULAR_FILE -> {
                    var regularFile = inodeTableRegion.createFile();
                    inode.addRegularFile(regularFile.getId(), fileName);
                    yield regularFile;
                }
                case UNKNOWN -> throw new IllegalArgumentException("Unsupported type");
            };
        }
    }

    public SeekableByteChannel newByteChannel(AtlantPath absolutePath, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {
        var atlant = options.contains(WRITE) ? AtlantFileChannel.openForWrite(path) : AtlantFileChannel.openForRead(path);
        Inode inode = null;
        try {
            inode = locate(absolutePath, FileType.REGULAR_FILE, options);
            if (options.contains(WRITE) || options.contains(APPEND)) {
                inode.beginWrite();
            } else {
                inode.beginRead();
            }
            Inode finalInode = inode;
            return new SeekableByteChannel() {

                private long position;

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    checkOpen();
                    if (position > finalInode.getSize()) {
                        throw new IOException("EOF reached");
                    }
                    return 0;
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    if (position >= finalInode.getSize() && !options.contains(APPEND)) {
                        throw new IOException("Opened as not APPEND");
                    }
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

                private void checkOpen() throws ClosedChannelException {
                    if (!isOpen()) {
                        throw new ClosedChannelException();
                    }
                }

                @Override
                public long size() {
                    return finalInode.getSize();
                }

                @Override
                public SeekableByteChannel truncate(long size) {
                    return this;
                }

                @Override
                public boolean isOpen() {
                    return false;
                }

                @Override
                public void close() throws IOException {
                    if (options.contains(WRITE) || options.contains(APPEND)) {
                        finalInode.endWrite();
                    } else {
                        finalInode.endRead();
                    }
                    atlant.close();
                }

            };
        } catch (IOException | AssertionError e) {
            atlant.close();
            if (inode != null) {
                if (options.contains(WRITE) || options.contains(APPEND)) {
                    inode.endWrite();
                } else {
                    inode.endRead();
                }
            }
            throw e;
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
        log.finer(() -> "Closing Atlant file system [path=" + path.toAbsolutePath() + "]...");
        assert AtlantFileChannel.notExists(); // TODO: Can be closed async?
        blockByteBuffer.remove();
        inodeByteBuffer.remove();
        isOpen = false;
        provider.removeFileSystem(path);
        log.fine(() -> "Successfully closed Atlant file system [path=" + path.toAbsolutePath() + "]");
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
    public WatchService newWatchService() throws IOException {
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

    Inode root() {
        return inodeTableRegion.root();
    }

    Inode findInode(Inode.Id inodeId) {
        return inodeTableRegion.get(inodeId);
    }

    Block.Id reserveBlock() throws BitmapRegionOutOfMemoryException {
        log.finer(() -> "Reserving 1 block...");
        var reserved = dataBitmapRegion.reserve();
        log.fine(() -> "Successfully reserved 1 block [" + reserved + "]");
        return reserved;
    }

    List<Block.Range> reserveBlocks(int size) throws BitmapRegionOutOfMemoryException {
        log.finer(() -> "Reserving [" + size + "] blocks...");
        var reserved = dataBitmapRegion.reserve(size);
        log.fine(() -> "Successfully reserved [" + size + "] block [" + reserved + "]");
        return reserved;
    }

    Inode.Id reserveInode() throws BitmapRegionOutOfMemoryException {
        log.finer(() -> "Reserving 1 inode...");
        var reserved = inodeBitmapRegion.reserve();
        log.fine(() -> "Successfully reserved 1 inode [" + reserved + "]");
        return reserved;
    }

    void freeBlock(Block.Id inodeId) {
        dataBitmapRegion.free(inodeId);
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

    ByteBuffer readInode(Inode.Id inodeId) {
        var buffer = getInodeByteBuffer();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position(inodePosition(inodeId));
            channel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    void writeBlock(Block.Id blockId, Consumer<ByteBuffer> consumer) {
        writeBlock(blockId, 0, consumer);
    }

    void writeBlock(Block.Id blockId, int offset, Consumer<ByteBuffer> consumer) {
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
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void writeInode(Inode.Id inodeId, Consumer<ByteBuffer> consumer) {
        var buffer = getInodeByteBuffer();
        consumer.accept(buffer);
        buffer.flip();
        var channel = AtlantFileChannel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position(inodePosition(inodeId));
            channel.write(buffer);
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
        try (var _ = AtlantFileChannel.openForRead(path)) {
            var inode = locate(absolutePath);
            // TODO: Add lock
            return AtlantFileAttributes.from(inode);
        }
    }

}

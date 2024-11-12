package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class AtlantFileSystem extends FileSystem {

    private static final Logger log = Logger.getLogger(AtlantFileSystem.class.getName());

    public static final String BLOCK_SIZE = "block-size";
    private static final ThreadLocal<ByteBuffer> blockByteBuffer = new ThreadLocal<>();
    private static final ThreadLocal<ByteBuffer> inodeByteBuffer = new ThreadLocal<>();
    private static final ThreadLocal<SeekableByteChannel> channel = new ThreadLocal<>();

    private final AtlantFileSystemProvider provider;
    private final Path path;
    private final SuperBlock superBlock;
    private final DataBitmapRegion dataBitmapRegion = new DataBitmapRegion(this);
    private final InodeBitmapRegion inodeBitmapRegion = new InodeBitmapRegion(this);
    private final InodeTableRegion inodeTableRegion;
    private volatile boolean isOpen = true;

    public AtlantFileSystem(AtlantFileSystemProvider provider, Path path, Map<String, ?> env) throws IOException {
        this.provider = provider;
        this.path = path;
        if (Files.exists(path)) {
            try (var channel = Files.newByteChannel(path, READ)) {
                AtlantFileSystem.channel.set(channel);
                var buffer = ByteBuffer.allocate(SuperBlock.LENGTH);
                channel.read(buffer);
                assert !buffer.hasRemaining();
                buffer.flip();
                this.superBlock = SuperBlock.read(buffer);
                this.inodeTableRegion = InodeTableRegion.read(this);
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to read Atlant file", e);
                throw e;
            } finally {
                AtlantFileSystem.channel.remove();
            }
        } else {
            this.superBlock = SuperBlock.withDefaults(env);
            try (var channel = Files.newByteChannel(path, READ, WRITE, CREATE)) {
                AtlantFileSystem.channel.set(channel);
                writeBlock(Block.Id.ZERO, superBlock::write);
                dataBitmapRegion.init();
                inodeBitmapRegion.init();
                this.inodeTableRegion = new InodeTableRegion(this);
            } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to create Atlant file", e);
                throw e;
            } finally {
                AtlantFileSystem.channel.remove();
            }
        }
    }

    void createDirectory(AtlantPath dir) throws IOException {
        try (var channel = Files.newByteChannel(this.path, READ, WRITE)) {
            AtlantFileSystem.channel.set(channel);
            var inode = root();
            for (Path path : dir) {
                var fileName = path.getFileName().toString();
                DirEntry dirEntry;
                try {
                    dirEntry = inode.get(fileName);
                } catch (NoSuchFileException e) {
                    var newInode = inodeTableRegion.createDirectory();
                    inode.addDirectory(newInode.getId(), fileName);
                    inode = newInode;
                    continue;
                }
                inode = findInode(dirEntry.getInode());
            }
        } finally {
            AtlantFileSystem.channel.remove();
        }
    }

    public DirectoryStream<Path> newDirectoryStream(AtlantPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var channel = Files.newByteChannel(path, READ);
        AtlantFileSystem.channel.set(channel);
        var inode = locate(dir);
        inode.readLock().lock();
        var iterator = inode.iterator();
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
                channel.close();
                AtlantFileSystem.channel.remove();
                inode.readLock().unlock();
            }
        };
    }

    private Inode locate(AtlantPath absolutePath) throws NoSuchFileException {
        var inode = inodeTableRegion.root();
        for (Path path : absolutePath) {
            try {
                var fileName = path.getFileName().toString();
                DirEntry dirEntry = inode.get(fileName);
                inode = findInode(dirEntry.getInode());
            } catch (NoSuchFileException e) {
                throw new NoSuchFileException(path.toString());
            }
        }
        return inode;
    }

    public <A extends BasicFileAttributes> SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {
        return null;
//        Object content = loadContent(path.toAbsolutePath().toString());
//        if (content instanceof List) {
//            throw new IOException("Is a directory");
//        }
//        String base64 = ((Map<String, String>) content).get("content");
//        final byte[] data = DatatypeConverter.parseBase64Binary(base64);
//        return new SeekableByteChannel() {
//            long position;
//
//            @Override
//            public int read(ByteBuffer dst) throws IOException {
//                int l = (int) Math.min(dst.remaining(), size() - position);
//                dst.put(data, (int) position, l);
//                position += l;
//                return l;
//            }
//
//            @Override
//            public int write(ByteBuffer src) throws IOException {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public long position() throws IOException {
//                return position;
//            }
//
//            @Override
//            public SeekableByteChannel position(long newPosition) throws IOException {
//                position = newPosition;
//                return this;
//            }
//
//            @Override
//            public long size() throws IOException {
//                return data.length;
//            }
//
//            @Override
//            public SeekableByteChannel truncate(long size) throws IOException {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public boolean isOpen() {
//                return true;
//            }
//
//            @Override
//            public void close() throws IOException {
//            }
//        };
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
        isOpen = false;
        provider.removeFileSystem(path);
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

    private boolean isRoot(String first) {
        for (Path path : getRootDirectories()) {
            if (path.toString().equals(first)) {
                return true;
            }
        }
        return false;
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
        return dataBitmapRegion.reserve();
    }

    Inode.Id reserveInode() throws BitmapRegionOutOfMemoryException {
        return inodeBitmapRegion.reserve();
    }

    void freeBlock(Block.Id inodeId) {
        dataBitmapRegion.free(inodeId);
    }

    void freeInode(Inode.Id inodeId) {
        inodeBitmapRegion.free(inodeId);
    }

    ByteBuffer readBlock(Block.Id blockId) {
        var buffer = getBlockByteBuffer();
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position((long) blockSize() * blockId.value());
            channel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    ByteBuffer readInode(Inode.Id inodeId) {
        var buffer = getInodeByteBuffer();
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position(superBlock.firstBlockOfInodeTables().value() + (long) inodeSize() * inodeId.value());
            channel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    void writeBlock(Block.Id blockId, Consumer<ByteBuffer> consumer) {
        var buffer = getBlockByteBuffer();
        consumer.accept(buffer);
        buffer.flip();
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position((long) blockSize() * blockId.value());
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void writeInode(Inode.Id inodeId, Consumer<ByteBuffer> consumer) {
        var buffer = getInodeByteBuffer();
        consumer.accept(buffer);
        buffer.flip();
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position((long) inodeSize() * inodeId.value());
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    SuperBlock superBlock() {
        return superBlock;
    }

    public AtlantFileAttributes readAttributes(AtlantPath path, LinkOption[] options) throws NoSuchFileException {
        var inode = locate((AtlantPath) path.getParent());
        return AtlantFileAttributes.from(inode);
    }

}

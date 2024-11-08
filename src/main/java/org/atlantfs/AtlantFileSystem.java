package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;

public class AtlantFileSystem extends FileSystem {

    public static final String BLOCK_SIZE = "block-size";
    private static final ThreadLocal<ByteBuffer> blockByteBuffer = new ThreadLocal<>();
    private static final ThreadLocal<ByteBuffer> inodeByteBuffer = new ThreadLocal<>();
    private static final ThreadLocal<SeekableByteChannel> channel = new ThreadLocal<>();

    private final AtlantFileSystemProvider provider;
    private final Path path;
    private final SuperBlock superBlock;
    private final DataBitmapRegion dataBitmapRegion = new DataBitmapRegion(this);
    private final InodeBitmapRegion inodeBitmapRegion = new InodeBitmapRegion(this);
    private final InodeTable inodeTable;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean isOpen = true;

    public AtlantFileSystem(AtlantFileSystemProvider provider, Path path, Map<String, ?> env) throws IOException {
        this.provider = provider;
        this.path = path;
        if (Files.exists(path)) {
            try (var channel = Files.newByteChannel(path, READ)) {
                AtlantFileSystem.channel.set(channel);
                var buffer = ByteBuffer.allocate(SuperBlock.LENGTH);
                this.superBlock = SuperBlock.read(buffer);
            } finally {
                AtlantFileSystem.channel.remove();
            }
        } else {
            int blockSize = Optional.ofNullable(env.get(BLOCK_SIZE))
                    .filter(Integer.class::isInstance)
                    .map(Integer.class::cast)
                    .orElseGet(AtlantFileSystem::getUnderlyingBlockSize);
            this.superBlock = new SuperBlock();
            superBlock.setBlockSize(blockSize);
            superBlock.setBlockSize(128);
            superBlock.setBlockBitmapsNumber(1);
            superBlock.setInodeBitmapsNumber(2);
            try (var channel = Files.newByteChannel(path, CREATE_NEW)) {
                AtlantFileSystem.channel.set(channel);
                superBlock.write(getBlockByteBuffer());
            } finally {
                AtlantFileSystem.channel.remove();
            }
        }
        this.inodeTable = new InodeTable(this);
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
            return buffer;
        }
        buffer = ByteBuffer.allocateDirect(capacity);
        threadLocal.set(buffer);
        return buffer;
    }

    int blockSize() {
        return superBlock.getBlockSize();
    }

    int inodeSize() {
        return superBlock.getInodeSize();
    }

    ByteBuffer readBlock(Block.Id block) {
        var buffer = getBlockByteBuffer();
        buffer.clear();
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position((long) blockSize() * block.value());
            channel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    void writeBlock(Block.Id block, Consumer<ByteBuffer> consumer) {
        var buffer = getBlockByteBuffer();
        buffer.clear();
        consumer.accept(buffer);
        var channel = AtlantFileSystem.channel.get();
        assert channel != null;
        assert channel.isOpen();
        try {
            channel.position((long) blockSize() * block.value());
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public DirectoryStream<Path> newDirectoryStream(AtlantPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        return null;
//        if (!dir.isAbsolute()) {
//            dir = dir.toAbsolutePath();
//        }
//        try (var channel = Files.newByteChannel(path, READ)) {
//            AtlantFileSystem.channel.set(channel);
//            var inode = inodeTable.get(Inode.Id.ROOT, channel);
//            for (Path path : dir) {
//                var name = path.toString();
//            }
//        } finally {
//            AtlantFileSystem.channel.remove();
//        }
//        final Object content = loadContent(dir.toAbsolutePath().toString());
//        if (content instanceof Map) {
//            throw new IOException("Is a file");
//        }
//        return new DirectoryStream<>() {
//            @Override
//            public Iterator<Path> iterator() {
//                return new Iterator<>() {
//                    final Iterator<Map<String, Object>> delegate = ((List<Map<String, Object>>) content).iterator();
//
//                    @Override
//                    public boolean hasNext() {
//                        return delegate.hasNext();
//                    }
//
//                    @Override
//                    public Path next() {
//                        Map<String, Object> val = delegate.next();
//                        return new AtlantPath(AtlantFileSystem.this, ((String) val.get("path")).getBytes(StandardCharsets.UTF_8));
//                    }
//
//                    @Override
//                    public void remove() {
//                        throw new UnsupportedOperationException();
//                    }
//                };
//            }
//
//            @Override
//            public void close() {
//            }
//        };
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
    public void close() throws IOException {
        beginWrite();
        try {
            if (!isOpen) {
                return;
            }
            isOpen = false;          // set closed
        } finally {
            endWrite();
        }
//        if (!streams.isEmpty()) {    // unlock and close all remaining streams
//            Set<InputStream> copy = new HashSet<>(streams);
//            for (InputStream is : copy) {
//                is.close();
//            }
//        }
//        beginWrite();                // lock and sync
//        try {
////            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
////                sync();
////                return null;
////            });
//            channel.close();              // close the ch just in case no update
//            // and sync didn't close the ch
//        } finally {
//            endWrite();
//        }
//
//        beginWrite();                // lock and sync
//        try {
//            // Clear the map so that its keys & values can be garbage collected
//            inodes = null;
//        } finally {
//            endWrite();
//        }
//
//        IOException ioe = null;
//        synchronized (tmppaths) {
//            for (Path p : tmppaths) {
//                try {
//                    AccessController.doPrivileged(
//                            (PrivilegedExceptionAction<Boolean>) () -> Files.deleteIfExists(p));
//                } catch (PrivilegedActionException e) {
//                    IOException x = (IOException) e.getException();
//                    if (ioe == null)
//                        ioe = x;
//                    else
//                        ioe.addSuppressed(x);
//                }
//            }
//        }
//        provider.removeFileSystem(zfpath, this);
//        if (ioe != null)
//            throw ioe;
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
        return null;
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
        return null;
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

    private void beginWrite() {
        lock.writeLock().lock();
    }

    private void endWrite() {
        lock.writeLock().unlock();
    }

    private static int getUnderlyingBlockSize() {
        try {
            return Math.toIntExact(FileSystems.getDefault()
                    .getFileStores()
                    .iterator()
                    .next()
                    .getBlockSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    SuperBlock getSuperBlock() {
        return superBlock;
    }

}

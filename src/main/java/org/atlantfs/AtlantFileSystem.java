package org.atlantfs;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;

public class AtlantFileSystem extends FileSystem {

    public static final String BLOCK_SIZE = "block-size";

    private final AtlantFileSystemProvider provider;
    private final SuperBlock superBlock;
    private final Bitmap blockBitmap;
    private final Bitmap inodeBitmap;
    //    private final InodeTable inodeTable;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean isOpen = true;

    public AtlantFileSystem(AtlantFileSystemProvider provider, Path path, Map<String, ?> env) throws IOException {
        this.provider = provider;
        if (Files.exists(path)) {
            try (SeekableByteChannel channel = Files.newByteChannel(path, READ)) {
                this.superBlock = SuperBlock.read(channel);
                int blockSize = superBlock.getBlockSize();
                this.blockBitmap = Bitmap.read(channel, 1 * blockSize, blockSize);
                this.inodeBitmap = Bitmap.read(channel, 2 * blockSize, blockSize);
            }
        } else {
            try (SeekableByteChannel channel = Files.newByteChannel(path, CREATE_NEW)) {
                int blockSize = Optional.ofNullable(env.get(BLOCK_SIZE))
                        .filter(Integer.class::isInstance)
                        .map(Integer.class::cast)
                        .orElseGet(AtlantFileSystem::getUnderlyingBlockSize);
                this.superBlock = SuperBlock.create(channel, blockSize);
                this.blockBitmap = Bitmap.create(channel, 1 * blockSize, blockSize);
                this.inodeBitmap = Bitmap.create(channel, 2 * blockSize, blockSize);
            }
        }
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

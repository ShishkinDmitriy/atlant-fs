package org.atlantfs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AtlantFileSystem extends FileSystem {

    private final AtlantFileSystemProvider fileSystemProvider;
    private final Path storage;
    private final RandomAccessFile writer;
    private final FileChannel channel;

    public AtlantFileSystem(AtlantFileSystemProvider fileSystemProvider, Path storage, Map<String, ?> env) throws IOException {
        this.fileSystemProvider = fileSystemProvider;
        this.storage = storage;
        String orDefault = Optional.ofNullable(env.get("mode")).map(Object::toString).orElse("rw");
        this.writer = new RandomAccessFile(storage.toAbsolutePath().toFile(), orDefault);
        this.channel = writer.getChannel();
    }

    @Override
    public FileSystemProvider provider() {
        return fileSystemProvider;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        channel.close();
    }

    @Override
    public boolean isOpen() {
        return false;
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
}

package org.atlantfs;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class AtlantFileChannel implements AutoCloseable {

    private static final ThreadLocal<AtlantFileChannel> registry = new ThreadLocal<>();
    private static final StandardOpenOption[] READ_OPTIONS = {READ};
    private static final StandardOpenOption[] WRITE_OPTIONS = {READ, WRITE};
    private static final StandardOpenOption[] CREATE_OPTIONS = {READ, WRITE, CREATE};

    private final boolean main;
    private final Set<OpenOption> options;
    private final SeekableByteChannel channel;

    private AtlantFileChannel(Path path, OpenOption... options) throws IOException {
        var existing = AtlantFileChannel.registry.get();
        if (existing == null) {
            this.main = true;
            this.options = new HashSet<>(Arrays.asList(options));
            this.channel = Files.newByteChannel(path, options);
            AtlantFileChannel.registry.set(this);
        } else {
            if (!existing.options.containsAll(Arrays.asList(options))) {
                throw new IllegalArgumentException("Increasing open options, should be only [" + existing.options + "] but were [" + Arrays.toString(options) + "]");
            }
            this.main = false;
            this.options = existing.options;
            this.channel = existing.channel;
        }
    }

    static AtlantFileChannel openForRead(Path path) throws IOException {
        return new AtlantFileChannel(path, READ_OPTIONS);
    }

    static AtlantFileChannel openForWrite(Path path) throws IOException {
        return new AtlantFileChannel(path, WRITE_OPTIONS);
    }

    static AtlantFileChannel openForCreate(Path path) throws IOException {
        return new AtlantFileChannel(path, CREATE_OPTIONS);
    }

    @Override
    public void close() throws IOException {
        if (!main) {
            return;
        }
        var existing = AtlantFileChannel.registry.get();
        assert existing != null;
        var channel = existing.channel;
        assert channel != null;
        channel.close();
        AtlantFileChannel.registry.remove();
    }

    public static boolean notExists() {
        return AtlantFileChannel.registry.get() == null;
    }

    public static SeekableByteChannel get() {
        var existing = AtlantFileChannel.registry.get();
        assert existing != null;
        return existing.channel;
    }

}

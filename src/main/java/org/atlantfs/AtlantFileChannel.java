package org.atlantfs;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class AtlantFileChannel implements AutoCloseable {

    private static final ThreadLocal<AtlantFileChannel> registry = new ThreadLocal<>();

    private final boolean main;
    private final Set<OpenOption> options;
    private final SeekableByteChannel channel;

    AtlantFileChannel(Path path, OpenOption... options) throws IOException {
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

    public static SeekableByteChannel get() {
        var existing = AtlantFileChannel.registry.get();
        assert existing != null;
        return existing.channel;
    }

}

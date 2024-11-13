package org.atlantfs;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class AtlantChannel implements AutoCloseable {

    private static final ThreadLocal<AtlantChannel> registry = new ThreadLocal<>();

    private final boolean main;
    private final Set<OpenOption> options;
    private final SeekableByteChannel channel;

    AtlantChannel(Path path, OpenOption... options) throws IOException {
        var existing = AtlantChannel.registry.get();
        if (existing == null) {
            this.main = true;
            this.options = new HashSet<>(Arrays.asList(options));
            this.channel = Files.newByteChannel(path, options);
            AtlantChannel.registry.set(this);
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
        var existing = AtlantChannel.registry.get();
        assert existing != null;
        var channel = existing.channel;
        assert channel != null;
        channel.close();
        AtlantChannel.registry.remove();
    }

    public static SeekableByteChannel get() {
        var existing = AtlantChannel.registry.get();
        assert existing != null;
        return existing.channel;
    }

}

package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

public class AtlantFileChannel implements SeekableByteChannel {

    private long position;
    private Inode inode;
    private Set<? extends OpenOption> options;
    private FileAttribute<?>[] attrs;
    private AtlantFileSystem fileSystem;

    int blockSize() {
        return fileSystem.blockSize();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        checkOpen();
        if (position > inode.getSize()) {
            throw new IOException("EOF reached");
        }
        var readLock = inode.readLock();
        try {
            readLock.lock();
        } finally {
            readLock.unlock();
        }
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (position >= inode.getSize()) {
            if (!options.contains(StandardOpenOption.APPEND)) {
                throw new IOException("Opened as not APPEND");
            }
            // Reserve if needed
            var blockNumber = inode.getSize() / blockSize();
            var remaining = src.remaining();
        }
        return 0;
    }

    @Override
    public long position() throws IOException {
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
    public long size() throws IOException {
        return inode.getSize();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return null;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }

}

package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class FileInode extends Inode<FileIblock> implements FileOperations {

    private static final Logger log = Logger.getLogger(FileInode.class.getName());

    FileInode(AtlantFileSystem fileSystem, Inode.Id id, FileIblock iBlock) {
        super(fileSystem, id, iBlock);
        checkInvariant();
    }

    static FileInode init(AtlantFileSystem fileSystem, Inode.Id id) {
        var dataIblock = DataIblock.init(fileSystem);
        return new FileInode(fileSystem, id, dataIblock);
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DataOutOfMemoryException, IndirectBlockOutOfMemoryException {
        try {
            beginWrite();
            var written = iBlock.write(position, buffer);
            flush();
            return written;
        } catch (DataOutOfMemoryException e) {
            upgradeInlineData();
            var written = iBlock.write(position, buffer);
            flush();
            return written;
        } finally {
            endWrite();
        }
    }

    @Override
    public int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException {
        try {
            beginRead();
            var read = iBlock.read(position, buffer);
            buffer.flip();
            return read;
        } finally {
            endRead();
        }
    }

    private void upgradeInlineData() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline data to block mapping...");
        assert iBlock instanceof DataIblock : "Only FILE_INLINE_DATA can be upgraded";
        var data = (DataIblock) iBlock;
        iBlock = FileBlockMapping.init(fileSystem, data.data());
        dirty = true;
        checkInvariant();
    }

}

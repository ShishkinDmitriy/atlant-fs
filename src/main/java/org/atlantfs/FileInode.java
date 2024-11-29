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
    public int write(long position, ByteBuffer buffer) throws NotEnoughSpaceException {
        try {
            beginWrite();
            var written = iblock.write(position, buffer);
            flush();
            return written;
        } catch (Data.NotEnoughSpaceException e) {
            upgradeInlineData();
            var written = iblock.write(position, buffer);
            flush();
            return written;
        } finally {
            endWrite();
        }
    }

    @Override
    public int read(long position, ByteBuffer buffer) {
        try {
            beginRead();
            var read = iblock.read(position, buffer);
            buffer.flip();
            return read;
        } finally {
            endRead();
        }
    }

    private void upgradeInlineData() throws BitmapRegion.NotEnoughSpaceException, IndirectBlock.NotEnoughSpaceException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline data to block mapping...");
        assert iblock instanceof DataIblock : "Only FILE_INLINE_DATA can be upgraded";
        var data = (DataIblock) iblock;
        iblock = FileBlockMapping.init(fileSystem, data.data());
        dirty = true;
        checkInvariant();
    }

}

package org.atlantfs;

import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.logging.Logger;

class DirInode extends Inode<DirIblock> implements DirectoryOperations {

    private static final Logger log = Logger.getLogger(DirInode.class.getName());

    DirInode(AtlantFileSystem fileSystem, Id id, DirIblock iblock) {
        super(fileSystem, id, iblock);
        checkInvariant();
    }

    static DirInode init(AtlantFileSystem fileSystem, DirInode.Id id) {
        var dirEntryListIblock = DirEntryListIblock.init(fileSystem);
        return new DirInode(fileSystem, id, dirEntryListIblock);
    }

    @Override
    public Iterator<DirEntry> iterator() {
        try {
            beginRead();
            return iBlock.iterator();
        } finally {
            endRead();
        }
    }

    @Override
    public DirEntry add(Inode.Id inode, FileType fileType, String name) throws AbstractOutOfMemoryException {
        try {
            beginWrite();
            var result = iBlock.add(inode, fileType, name);
            flush();
            return result;
        } catch (DirEntryListOfMemoryException e) {
            upgradeInlineDirList();
            var result = iBlock.add(inode, fileType, name);
            flush();
            return result;
        } finally {
            endWrite();
        }
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        try {
            beginRead();
            return iBlock.get(name);
        } finally {
            endRead();
        }
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, AbstractOutOfMemoryException {
        try {
            beginWrite();
            iBlock.rename(name, newName);
            flush();
        } catch (DirEntryListOfMemoryException e) {
            upgradeInlineDirList();
            iBlock.rename(name, newName);
            flush();
        } finally {
            endWrite();
        }
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        try {
            beginWrite();
            iBlock.delete(name);
            flush();
        } finally {
            endWrite();
        }
    }

    private void upgradeInlineDirList() throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline dir list to block mapping...");
        assert iBlock instanceof DirEntryListIblock : "Only DIR_INLINE_LIST can be upgraded";
        var dirEntryList = (DirEntryListIblock) iBlock;
        iBlock = DirBlockMapping.init(fileSystem, dirEntryList.entryList());
        dirty = true;
        checkInvariant();
    }

}

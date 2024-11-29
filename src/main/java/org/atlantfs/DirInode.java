package org.atlantfs;

import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.logging.Logger;

class DirInode extends Inode<DirIblock> implements DirOperations {

    private static final Logger log = Logger.getLogger(DirInode.class.getName());

    DirInode(AtlantFileSystem fileSystem, Id id, DirIblock iblock) {
        super(fileSystem, id, iblock);
        checkInvariant();
    }

    static DirInode init(AtlantFileSystem fileSystem, DirInode.Id id) {
        var dirEntryListIblock = DirListIblock.init(fileSystem);
        return new DirInode(fileSystem, id, dirEntryListIblock);
    }

    @Override
    public Iterator<DirEntry> iterator() {
        try {
            beginRead();
            return iblock.iterator();
        } finally {
            endRead();
        }
    }

    @Override
    public DirEntry add(Inode.Id id, FileType fileType, String name) throws NotEnoughSpaceException {
        try {
            beginWrite();
            var result = iblock.add(id, fileType, name);
            flush();
            return result;
        } catch (DirListNotEnoughSpaceException e) {
            upgradeInlineDirList();
            var result = iblock.add(id, fileType, name);
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
            return iblock.get(name);
        } finally {
            endRead();
        }
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, NotEnoughSpaceException {
        try {
            beginWrite();
            iblock.rename(name, newName);
            flush();
        } catch (DirListNotEnoughSpaceException e) {
            upgradeInlineDirList();
            iblock.rename(name, newName);
            flush();
        } finally {
            endWrite();
        }
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        try {
            beginWrite();
            iblock.delete(name);
            flush();
        } finally {
            endWrite();
        }
    }

    private void upgradeInlineDirList() throws BitmapRegionNotEnoughSpaceException, IndirectBlockNotEnoughSpaceException {
        log.fine(() -> "Upgrading inode [id=" + id + "] from inline dir list to block mapping...");
        assert iblock instanceof DirListIblock : "Only DIR_INLINE_LIST can be upgraded";
        var dirEntryList = (DirListIblock) iblock;
        iblock = DirBlockMapping.init(fileSystem, dirEntryList.entryList());
        dirty = true;
        checkInvariant();
    }

}

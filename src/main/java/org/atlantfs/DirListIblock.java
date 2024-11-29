package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirListIblock implements DirIblock {

    private final DirList entryList;

    private DirListIblock(DirList entryList) {
        this.entryList = entryList;
    }

    static DirListIblock init(AtlantFileSystem fileSystem) {
        var dirEntryList = DirList.init(fileSystem.iblockSize());
        return init(fileSystem, dirEntryList);
    }

    static DirListIblock init(AtlantFileSystem fileSystem, DirList dirList) {
        dirList.resize(fileSystem.iblockSize());
        return new DirListIblock(dirList);
    }

    static DirListIblock read(ByteBuffer buffer) {
        var dirEntryList = DirList.read(buffer);
        return new DirListIblock(dirEntryList);
    }

    @Override
    public void flush(ByteBuffer buffer) {
        entryList.flush(buffer);
    }

    DirList entryList() {
        return entryList;
    }

    @Override
    public Iterator<DirEntry> iterator() {
        return entryList.iterator();
    }

    @Override
    public DirEntry add(Inode.Id id, FileType fileType, String name) throws DirNotEnoughSpaceException, BitmapRegionNotEnoughSpaceException {
        return entryList.add(id, fileType, name);
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        return entryList.get(name);
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirNotEnoughSpaceException, BitmapRegionNotEnoughSpaceException {
        entryList.rename(name, newName);
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        entryList.delete(name);
    }

    @Override
    public void delete() throws DirectoryNotEmptyException {
    }

    @Override
    public IblockType type() {
        return IblockType.DIR_INLINE_LIST;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int blocksCount() {
        return 0;
    }

}

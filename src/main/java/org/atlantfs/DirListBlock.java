package org.atlantfs;

import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirListBlock implements Block, DirOperations {

    private final Id id;
    private final DirList entryList;
    private final AtlantFileSystem fileSystem;

    private DirListBlock(Id id, AtlantFileSystem fileSystem, DirList entryList) {
        this.id = id;
        this.entryList = entryList;
        this.fileSystem = fileSystem;
    }

    static DirListBlock init(AtlantFileSystem fileSystem) throws BitmapRegionNotEnoughSpaceException {
        var dirEntryList = DirList.init(fileSystem.blockSize());
        return init(fileSystem, dirEntryList);
    }

    static DirListBlock init(AtlantFileSystem fileSystem, DirList dirList) throws BitmapRegionNotEnoughSpaceException {
        var reserved = fileSystem.reserveBlock();
        dirList.resize(fileSystem.blockSize());
        return new DirListBlock(reserved, fileSystem, dirList);
    }

    static DirListBlock read(AtlantFileSystem fileSystem, Id id) {
        var buffer = fileSystem.readBlock(id);
        var dirEntryList = DirList.read(buffer);
        return new DirListBlock(id, fileSystem, dirEntryList);
    }

    @Override
    public Id id() {
        return id;
    }

    @Override
    public boolean isDirty() {
        return entryList.isDirty();
    }

    @Override
    public void flush() {
        if (!isDirty()) {
            return;
        }
        fileSystem.writeBlock(id, entryList::flush);
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

}

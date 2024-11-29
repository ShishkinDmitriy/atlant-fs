package org.atlantfs;

import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirListBlock implements Block, DirOperations {

    private final Id id;
    private final DirList dirList;
    private final AtlantFileSystem fileSystem;

    private DirListBlock(Id id, AtlantFileSystem fileSystem, DirList dirList) {
        this.id = id;
        this.fileSystem = fileSystem;
        this.dirList = dirList;
    }

    static DirListBlock init(AtlantFileSystem fileSystem) throws BitmapRegion.NotEnoughSpaceException {
        var dirEntryList = DirList.init(fileSystem.blockSize());
        return init(fileSystem, dirEntryList);
    }

    static DirListBlock init(AtlantFileSystem fileSystem, DirList dirList) throws BitmapRegion.NotEnoughSpaceException {
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
        return dirList.isDirty();
    }

    @Override
    public void flush() {
        if (!isDirty()) {
            return;
        }
        fileSystem.writeBlock(id, dirList::flush);
    }

    DirList dirList() {
        return dirList;
    }

    @Override
    public Iterator<DirEntry> iterator() {
        return dirList.iterator();
    }

    @Override
    public DirEntry add(Inode.Id id, FileType fileType, String name) throws DirList.NotEnoughSpaceException {
        return dirList.add(id, fileType, name);
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        return dirList.get(name);
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirList.NotEnoughSpaceException {
        dirList.rename(name, newName);
    }

    @Override
    public void remove(String name) throws NoSuchFileException {
        dirList.remove(name);
    }

    @Override
    public void delete() throws DirectoryNotEmptyException {
    }

}

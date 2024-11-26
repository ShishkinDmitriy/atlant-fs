package org.atlantfs;

import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirEntryListBlock implements Block, DirectoryOperations {

    private final Id id;
    private final DirEntryList entryList;
    private final AtlantFileSystem fileSystem;

    private DirEntryListBlock(Id id, AtlantFileSystem fileSystem, DirEntryList entryList) {
        this.id = id;
        this.entryList = entryList;
        this.fileSystem = fileSystem;
    }

    static DirEntryListBlock init(AtlantFileSystem fileSystem) throws BitmapRegionOutOfMemoryException {
        var dirEntryList = DirEntryList.init(fileSystem.blockSize());
        return init(fileSystem, dirEntryList);
    }

    static DirEntryListBlock init(AtlantFileSystem fileSystem, DirEntryList dirEntryList) throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlock();
        dirEntryList.resize(fileSystem.blockSize());
        return new DirEntryListBlock(reserved, fileSystem, dirEntryList);
    }

    static DirEntryListBlock read(AtlantFileSystem fileSystem, Id id) {
        var buffer = fileSystem.readBlock(id);
        var dirEntryList = DirEntryList.read(buffer);
        return new DirEntryListBlock(id, fileSystem, dirEntryList);
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

    DirEntryList entryList() {
        return entryList;
    }

    @Override
    public Iterator<DirEntry> iterator() {
        return entryList.iterator();
    }

    @Override
    public DirEntry add(Inode.Id inode, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        return entryList.add(inode, fileType, name);
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        return entryList.get(name);
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
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

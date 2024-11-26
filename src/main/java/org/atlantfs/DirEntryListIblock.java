package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirEntryListIblock implements DirIblock {

    private final DirEntryList entryList;

    private DirEntryListIblock(DirEntryList entryList) {
        this.entryList = entryList;
    }

    static DirEntryListIblock init(AtlantFileSystem fileSystem) {
        var dirEntryList = DirEntryList.init(fileSystem.iblockSize());
        return init(fileSystem, dirEntryList);
    }

    static DirEntryListIblock init(AtlantFileSystem fileSystem, DirEntryList dirEntryList) {
        dirEntryList.resize(fileSystem.iblockSize());
        return new DirEntryListIblock(dirEntryList);
    }

    static DirEntryListIblock read(ByteBuffer buffer) {
        var dirEntryList = DirEntryList.read(buffer);
        return new DirEntryListIblock(dirEntryList);
    }

    @Override
    public void flush(ByteBuffer buffer) {
        entryList.flush(buffer);
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

    @Override
    public IBlockType type() {
        return IBlockType.DIR_INLINE_LIST;
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

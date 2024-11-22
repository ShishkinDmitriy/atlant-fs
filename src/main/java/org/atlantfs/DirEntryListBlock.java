package org.atlantfs;

public class DirEntryListBlock implements Block {

    private final Id id;
    private final DirEntryList entryList;
    private final AtlantFileSystem fileSystem;

    private DirEntryListBlock(Id id, AtlantFileSystem fileSystem, DirEntryList entryList) {
        this.id = id;
        this.entryList = entryList;
        this.fileSystem = fileSystem;
    }

    static DirEntryListBlock init(AtlantFileSystem fileSystem) throws BitmapRegionOutOfMemoryException {
        var reserved = fileSystem.reserveBlock();
        var dirEntryList = DirEntryList.init(fileSystem.blockSize());
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

}

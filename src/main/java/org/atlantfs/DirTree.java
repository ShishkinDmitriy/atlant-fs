package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

class DirTree implements DirOperations {

    private DirTreeNode root;
    private final AtlantFileSystem fileSystem;

    DirTree(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    static DirTree read(AtlantFileSystem fileSystem, ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DirEntry> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirEntry add(Inode.Id id, FileType fileType, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        var block = root.get(name);
        for (var i = 0; i < root.getDepth(); i++) {
            var buffer = fileSystem.readBlock(block);
            var dirTreeNode = DirTreeNode.read(buffer);
            block = dirTreeNode.get(name);
        }
        var buffer = fileSystem.readBlock(block);
        var dirEntryList = DirList.read(buffer);
        return dirEntryList.get(name);
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(String name) throws NoSuchFileException {
        throw new UnsupportedOperationException();
    }

}

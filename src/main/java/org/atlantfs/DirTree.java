package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

class DirTree {

    private DirTreeNode root;
    private final AtlantFileSystem fileSystem;

    DirTree(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

//    static DirTree read(AtlantFileSystem fileSystem, ByteBuffer buffer) {
//        return new DirTree(fileSystem);
//    }

    DirEntry get(String name) throws IOException {
        var block = root.get(name);
        for (var i = 0; i < root.getDepth(); i++) {
            var buffer = fileSystem.readBlock(block);
            var dirTreeNode = DirTreeNode.read(buffer);
            block = dirTreeNode.get(name);
        }
        var buffer = fileSystem.readBlock(block);
        var dirEntryList = DirEntryList.read(buffer);
        return dirEntryList.get(name);
    }

}

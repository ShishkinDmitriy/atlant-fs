package org.atlantfs;

import java.nio.file.NoSuchFileException;
import java.util.Iterator;

interface DirectoryOperations {

    Iterator<DirEntry> iterator();

    default DirEntry addDirectory(Inode.Id inode, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        return add(inode, FileType.DIRECTORY, name);
    }

    default DirEntry addRegularFile(Inode.Id inode, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        return add(inode, FileType.REGULAR_FILE, name);
    }

    DirEntry add(Inode.Id inode, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException;

    DirEntry get(String name) throws NoSuchFileException;

    void rename(String name, String newName) throws NoSuchFileException, DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException;

    void delete(String name) throws NoSuchFileException;

}

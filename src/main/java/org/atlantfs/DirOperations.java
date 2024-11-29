package org.atlantfs;

import java.nio.file.NoSuchFileException;
import java.util.Iterator;

interface DirOperations {

    Iterator<DirEntry> iterator();

    default DirEntry addDir(Inode.Id id, String name) throws NotEnoughSpaceException {
        return add(id, FileType.DIRECTORY, name);
    }

    default DirEntry addFile(Inode.Id id, String name) throws NotEnoughSpaceException {
        return add(id, FileType.REGULAR_FILE, name);
    }

    DirEntry add(Inode.Id id, FileType fileType, String name) throws NotEnoughSpaceException;

    DirEntry get(String name) throws NoSuchFileException;

    void rename(String name, String newName) throws NoSuchFileException, NotEnoughSpaceException;

    void delete(String name) throws NoSuchFileException;

}

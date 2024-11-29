package org.atlantfs;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class AtlantFileAttributes implements BasicFileAttributes {

    private final Inode<?> inode;

    private AtlantFileAttributes(Inode<?> inode) {
        this.inode = inode;
    }

    static AtlantFileAttributes from(Inode<?> inode) {
        return new AtlantFileAttributes(inode);
    }

    @Override
    public FileTime lastModifiedTime() {
        return null;
    }

    @Override
    public FileTime lastAccessTime() {
        return null;
    }

    @Override
    public FileTime creationTime() {
        return null;
    }

    @Override
    public boolean isRegularFile() {
        return inode instanceof FileOperations;
    }

    @Override
    public boolean isDirectory() {
        return inode instanceof DirOperations;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return inode.size();
    }

    @Override
    public Object fileKey() {
        return inode.getId();
    }

}

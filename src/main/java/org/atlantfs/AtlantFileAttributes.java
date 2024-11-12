package org.atlantfs;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class AtlantFileAttributes implements BasicFileAttributes {

    private final Inode.Id id;
    private final long size;
    private final FileType fileType;

    private AtlantFileAttributes(Inode.Id id, long size, FileType fileType) {
        this.id = id;
        this.size = size;
        this.fileType = fileType;
    }

    static AtlantFileAttributes from(Inode inode) {
        return new AtlantFileAttributes(inode.getId(), inode.getSize(), inode.getFileType());
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
        return fileType == FileType.REGULAR_FILE;
    }

    @Override
    public boolean isDirectory() {
        return fileType == FileType.DIRECTORY;
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
        return size;
    }

    @Override
    public Object fileKey() {
        return id;
    }

}

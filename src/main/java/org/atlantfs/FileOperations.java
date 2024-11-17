package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;

interface FileOperations {

    int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException, DataOutOfMemoryException;

    int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException;

    void delete() throws DirectoryNotEmptyException;

}

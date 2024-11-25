package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;

interface FileOperations {

    int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DataOutOfMemoryException, IndirectBlockOutOfMemoryException;

    int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException;

    void delete() throws IOException;

}

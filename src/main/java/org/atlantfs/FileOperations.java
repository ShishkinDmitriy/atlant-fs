package org.atlantfs;

import java.nio.ByteBuffer;

interface FileOperations {

    int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DataOutOfMemoryException, IndirectBlockOutOfMemoryException;

    int read(long position, ByteBuffer buffer) throws DataOutOfMemoryException;

}

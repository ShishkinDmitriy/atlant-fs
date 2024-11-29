package org.atlantfs;

import java.nio.ByteBuffer;

interface FileOperations {

    int write(long position, ByteBuffer buffer) throws BitmapRegionNotEnoughSpaceException, DataNotEnoughSpaceException, IndirectBlockNotEnoughSpaceException;

    int read(long position, ByteBuffer buffer) throws DataNotEnoughSpaceException;

}

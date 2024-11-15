package org.atlantfs;

import java.nio.ByteBuffer;

interface FileOperations {

    int write(long position, ByteBuffer buffer);

    int read(long position, ByteBuffer buffer);

}

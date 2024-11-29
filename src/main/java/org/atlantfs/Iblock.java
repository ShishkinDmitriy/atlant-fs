package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;

interface Iblock {

    long size();

    int blocksCount();

    void flush(ByteBuffer buffer);

    IblockType type();

    void delete() throws IOException;

}

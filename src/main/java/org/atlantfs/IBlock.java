package org.atlantfs;

import java.nio.ByteBuffer;

interface IBlock {

    long size();

    int blocksCount();

    void flush(ByteBuffer buffer);

    IBlockType type();

}

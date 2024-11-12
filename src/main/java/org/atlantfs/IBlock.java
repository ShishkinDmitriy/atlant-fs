package org.atlantfs;

import java.nio.ByteBuffer;

interface IBlock {

    void write(ByteBuffer buffer);

}

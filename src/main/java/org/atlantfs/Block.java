package org.atlantfs;

sealed class Block permits BlockData, BlockDirEntity {

    protected int number;
    protected int length;

}

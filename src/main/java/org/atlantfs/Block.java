package org.atlantfs;

sealed class Block permits BlockData {

    protected int number;
    protected int length;

    record Id(int value) {
    }

}

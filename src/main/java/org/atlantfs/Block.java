package org.atlantfs;

sealed class Block permits BlockData {

    protected int number;
    protected int length;

    record Id(int value) {

        static Id of(int value) {
            return new Id(value);
        }

    }

}

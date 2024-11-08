package org.atlantfs;

interface AbstractRange<K extends AbstractId> {

    K from();

    int length();

}

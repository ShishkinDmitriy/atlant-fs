package org.atlantfs;

interface Range<K extends Id> {

    K from();

    int length();

}

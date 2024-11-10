package org.atlantfs;

public class DirEntryListOfMemoryException extends DirectoryOutOfMemoryException {

    public DirEntryListOfMemoryException() {
    }

    public DirEntryListOfMemoryException(String message) {
        super(message);
    }

    public DirEntryListOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirEntryListOfMemoryException(Throwable cause) {
        super(cause);
    }

}

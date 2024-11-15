package org.atlantfs;

public class DirectoryOutOfMemoryException extends AtlantFileSystemException {

    public DirectoryOutOfMemoryException() {
    }

    public DirectoryOutOfMemoryException(String message) {
        super(message);
    }

    public DirectoryOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirectoryOutOfMemoryException(Throwable cause) {
        super(cause);
    }

}

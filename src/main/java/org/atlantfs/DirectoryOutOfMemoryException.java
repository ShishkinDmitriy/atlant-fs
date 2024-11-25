package org.atlantfs;

public class DirectoryOutOfMemoryException extends AbstractOutOfMemoryException {

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

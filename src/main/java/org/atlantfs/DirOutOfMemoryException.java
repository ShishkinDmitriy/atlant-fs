package org.atlantfs;

public class DirOutOfMemoryException extends AbstractOutOfMemoryException {

    public DirOutOfMemoryException() {
    }

    public DirOutOfMemoryException(String message) {
        super(message);
    }

    public DirOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirOutOfMemoryException(Throwable cause) {
        super(cause);
    }

}

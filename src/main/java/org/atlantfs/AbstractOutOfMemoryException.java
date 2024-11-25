package org.atlantfs;

public class AbstractOutOfMemoryException extends AtlantFileSystemException {
    public AbstractOutOfMemoryException() {
    }

    public AbstractOutOfMemoryException(String message) {
        super(message);
    }

    public AbstractOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public AbstractOutOfMemoryException(Throwable cause) {
        super(cause);
    }
}

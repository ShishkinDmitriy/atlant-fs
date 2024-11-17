package org.atlantfs;

public class DataOutOfMemoryException extends AtlantFileSystemException {

    public DataOutOfMemoryException() {
    }

    public DataOutOfMemoryException(String message) {
        super(message);
    }

    public DataOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataOutOfMemoryException(Throwable cause) {
        super(cause);
    }

}

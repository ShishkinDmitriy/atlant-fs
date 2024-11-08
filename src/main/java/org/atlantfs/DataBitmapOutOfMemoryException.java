package org.atlantfs;

public class DataBitmapOutOfMemoryException extends AtlantFileSystemException {

    public DataBitmapOutOfMemoryException() {
    }

    public DataBitmapOutOfMemoryException(String message) {
        super(message);
    }

    public DataBitmapOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBitmapOutOfMemoryException(Throwable cause) {
        super(cause);
    }

}
